package gostream

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/segmentio/ksuid"
	"github.com/sethvargo/go-retry"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"
)

var schemaTemplate *template.Template

const schemaRawTemplate = `
CREATE TABLE IF NOT EXISTS "{{ .OutboxTableName }}" (
	idempotency_key TEXT PRIMARY KEY NOT NULL,
	stream TEXT NOT NULL,
	payload BYTEA NOT NULL,
	created_at TIMESTAMPTZ NOT NULL
)

CREATE INDEX IF NOT EXISTS "{{ .OutboxTableName }}_idx_redis_outbox" ON "{{ .OutboxTableName }}" USING BTREE ("created_at");
`

func init() {
	schemaTemplate = template.Must(template.New("schema").Parse(schemaRawTemplate))
}

func getSchema(cfg *redisStreamConfig) string {
	b := bytes.NewBuffer([]byte{})
	if err := schemaTemplate.Execute(b, map[string]string{"OutboxTableName": cfg.outboxTableName}); err != nil {
		panic("error executing schemaTemplate: " + err.Error())
	}
	return b.String()
}

type redisStreamConfig struct {
	outboxTableName string
	useOutboxTable  bool
	sqlDriverName   string
}

func WithUseOutboxTable(useOutboxTable bool) RedisStreamOption {
	return func(cfg *redisStreamConfig) error {
		cfg.useOutboxTable = useOutboxTable
		return nil
	}
}

func WithOutboxTableName(outboxTableName string) RedisStreamOption {
	return func(cfg *redisStreamConfig) error {
		if len(outboxTableName) == 0 {
			return errors.New("outboxTableName is empty")
		}
		cfg.outboxTableName = outboxTableName
		return nil
	}
}

func WithSQLDriverName(driverName string) RedisStreamOption {
	return func(cfg *redisStreamConfig) error {
		if len(driverName) == 0 {
			return errors.New("driverName is empty")
		}
		cfg.sqlDriverName = driverName
		return nil
	}
}

type RedisStreamOption func(cfg *redisStreamConfig) error

type RedisStream struct {
	db               *sql.DB
	cfg              *redisStreamConfig
	ctx              context.Context
	log              *zap.Logger
	redisClient      *redis.Client
	emptyOutbox      chan struct{}
	schemaCreateOnce sync.Once
	clientID         string
}

type OutboxTable struct {
	IdempotencyKey string    `json:"idempotency_key"`
	Stream         string    `db:"stream" json:"stream_name"`
	Payload        []byte    `db:"payload" json:"payload"`
	CreatedAt      time.Time `db:"created_at" json:"created_at"`
}

type Message struct {
	ID             string    `json:"uuid"`
	IdempotencyKey string    `json:"idempotency_key"`
	Stream         string    `db:"stream" json:"stream_name"`
	Payload        []byte    `db:"payload" json:"payload"`
	CreatedAt      time.Time `db:"created_at" json:"created_at"`
}

func (r *RedisStream) triggerEmptyOutbox() {
	r.emptyOutbox <- struct{}{}
}

func (r *RedisStream) PublishMessage(ctx context.Context, tx *sql.Tx, streamName string, payload []byte) (string, error) {
	createdAt := time.Now()
	idempotencyKey, err := ksuid.NewRandom()
	if err != nil {
		return "", err
	}
	_, err = tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (idempotency_key, stream, payload, created_at) VALUES ($1, $2, $3, $4);", r.cfg.outboxTableName), idempotencyKey.String(), streamName, payload, createdAt)
	if err == nil {
		go func() {
			time.Sleep(time.Millisecond * 50)
			r.triggerEmptyOutbox()
		}()
	}
	return idempotencyKey.String(), err
}

func (r *RedisStream) emptyOutboxRun(ctx context.Context) {
	for {
		var breakLoop bool
		backoff, err := retry.NewFibonacci(500 * time.Millisecond)
		if err != nil {
			r.log.Error("Backoff error", zap.Error(err))
		}
		backoff = retry.WithCappedDuration(time.Second*5, backoff)

		err = retry.Do(ctx, backoff, func(ctx context.Context) error {
			// TODO: get message
			tx, err := r.db.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			entry := &OutboxTable{}
			err = tx.QueryRowContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE idempotency_key = (SELECT idempotency_key FROM %s ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING idempotency_key, stream, payload, created_at", r.cfg.outboxTableName, r.cfg.outboxTableName)).
				Scan(&entry.IdempotencyKey, &entry.Stream, &entry.Payload, &entry.CreatedAt)
			if err != nil {
				if err == sql.ErrNoRows {
					breakLoop = true
					return nil
				}
				r.log.Error("Error getting the row", zap.Error(err))
				return err
			}
			// 			s.log.Info("Found a message, sending to Redis now", zap.Reflect("entry", entry))
			err = r.redisClient.XAdd(ctx, &redis.XAddArgs{Stream: entry.Stream, Values: map[string]interface{}{
				"idempotency_key": entry.IdempotencyKey,
				"payload":         base64.StdEncoding.EncodeToString(entry.Payload),
				"created_at":      entry.CreatedAt.Unix(),
			}}).Err()
			if err != nil {
				return err
			}
			return tx.Commit()
		})
		if err != nil {
			r.log.Error("emptyOutboxRun error", zap.Error(err))
			breakLoop = true
		}
		if breakLoop {
			break
		}
	}

}

func (r *RedisStream) emptyOutboxLoop() {
	duration := time.Second * 2
	ticker := time.NewTimer(duration)

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(r.ctx, duration*10)
			r.emptyOutboxRun(ctx)
			cancel()
			ticker.Reset(duration)
		case <-r.emptyOutbox:
			ctx, cancel := context.WithTimeout(r.ctx, duration*10)
			r.emptyOutboxRun(ctx)
			cancel()
			ticker.Reset(duration)
		case <-r.ctx.Done():
			r.log.Info("Stopping RedisStream.emptyOutbox ticker")
			return
		}
	}
}

func (r *RedisStream) init() {
	if r.cfg.useOutboxTable {
		go r.emptyOutboxLoop()
	}
}

func (r *RedisStream) ensureGroupCreated(ctx context.Context, stream, group string) error {
	_, err := r.redisClient.XGroupCreateMkStream(ctx, stream, group, "0").Result()
	if err != nil && strings.Contains(err.Error(), "already exists") {
		return nil
	}
	return err
}

// TODO: non blocking
func (r *RedisStream) Subscribe(ctx context.Context, group string, stream string, f func(msg *Message) error) error {
	if err := r.ensureGroupCreated(ctx, stream, group); err != nil {
		return err
	}
	for {
		streamResult, err := r.redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{Consumer: r.clientID, Group: group, Streams: []string{stream, ">"}, Count: 1, Block: time.Second * 5}).Result()
		if err != nil {
			if err == redis.Nil {
				continue
			}
			r.log.Error("Cannot read streamResult group. Sleeping 3 seconds...", zap.String("streamResult", stream), zap.String("group", group), zap.Error(err))
			time.Sleep(time.Second * 3)
			continue
		}

		msg := streamResult[0].Messages[0]
		streamName := streamResult[0].Stream

		payload, _ := base64.StdEncoding.DecodeString(msg.Values["payload"].(string))
		ts, _ := strconv.ParseInt(msg.Values["created_at"].(string), 10, 64)
		backoff, _ := retry.NewConstant(time.Second)
		err = retry.Do(ctx, retry.WithMaxDuration(time.Second*10, backoff), func(ctx context.Context) error {
			if err := f(&Message{
				ID:             msg.ID,
				IdempotencyKey: msg.Values["idempotency_key"].(string),
				Stream:         streamName,
				Payload:        payload,
				CreatedAt:      time.Unix(ts, 0),
			}); err != nil {
				return retry.RetryableError(err)
			}
			return nil
		})
		if err == nil {
			backoff, _ := retry.NewConstant(time.Second)
			if err := retry.Do(ctx, retry.WithMaxDuration(time.Second*10, backoff), func(ctx context.Context) error {
				return retry.RetryableError(r.redisClient.XAck(ctx, streamName, group, msg.ID).Err())
			}); err != nil {
				r.log.Error("Could not ack the redis message",
					zap.String("stream", streamName),
					zap.String("group", group),
					zap.String("msg.ID", msg.ID),
				)
			}
			r.log.Info("Message processed successfuly", zap.String("stream", streamName),
				zap.String("group", group),
				zap.String("msg.ID", msg.ID))
		} else {
			r.log.Error("Could not process message after several retries",
				zap.String("stream", streamName),
				zap.String("group", group),
				zap.String("msg.ID", msg.ID),
			)
		}

	}

}

func NewRedisStream(ctx context.Context, clientID string, redisHostPort string, db *sql.DB, log *zap.Logger, options ...RedisStreamOption) (*RedisStream, error) {
	if db == nil {
		return nil, errors.New("db is nil")
	}

	// default config
	cfg := &redisStreamConfig{
		useOutboxTable:  true,
		outboxTableName: "redis_outbox",
		sqlDriverName:   "postgres",
	}
	for _, option := range options {
		if err := option(cfg); err != nil {
			return nil, err
		}
	}

	r := &RedisStream{
		ctx:      ctx,
		clientID: clientID,
		db:       db,
		cfg:      cfg,
		log:      log.Named("RedisStream"),
		redisClient: redis.NewClient(&redis.Options{
			Addr: redisHostPort,
		}),
		emptyOutbox:      make(chan struct{}),
		schemaCreateOnce: sync.Once{},
	}
	r.init()
	return r, nil
}
