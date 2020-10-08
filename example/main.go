package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	_ "github.com/lib/pq"
	"github.com/rooka-app/gostream"
	"github.com/segmentio/ksuid"
	"go.uber.org/zap"
	"math/rand"
	"time"
)

func main() {
	streamName := flag.String("n", "", "stream name")
	consumer := flag.Bool("c", false, "consumer")
	publisher := flag.Bool("p", false, "publisher")
	consumerGroup := flag.String("g", "", "consumer group")
	msgs := flag.Int("m", 1, "publish messages")
	flag.Parse()

	log, _ := zap.NewDevelopment()
	if *streamName == "" {
		log.Panic("streamName empty")
	}

	db, err := sql.Open("postgres", "postgres://postgres:postgres@localhost:5432/rooka?sslmode=disable")
	if err != nil {
		log.Panic("sql.Open error", zap.Error(err))
	}
	db.SetMaxOpenConns(50)
	defer db.Close()
	clientID, _ := ksuid.NewRandom()
	stream, err := gostream.NewRedisStream(context.Background(), clientID.String(), "localhost:6379", db, log)
	if err != nil {
		log.Panic("NewRedisStream error", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if *consumer {
		log.Info("Consumer", zap.String("streamName", *streamName))
		err := stream.Subscribe(ctx, *consumerGroup, *streamName, func(msg *gostream.Message) error {
			log.Info("Got new message", zap.String("id", msg.ID), zap.String("msg.payload", string(msg.Payload)))

			if rand.Intn(2) == 0 {
				return nil
			}

			log.Error("Subscribe error happened for message", zap.String("msg.ID", msg.ID))
			return errors.New("error consuming")
		})
		if err != nil {
			log.Panic(err.Error())
		}
	} else if *publisher {
		log.Info("Publisher", zap.String("streamName", *streamName))
		for i := 0; i < *msgs; i++ {
			tx, err := db.BeginTx(context.Background(), nil)
			if err != nil {
				log.Panic("tx error", zap.Error(err))
			}
			defer tx.Rollback()
			_, err = stream.PublishMessage(context.Background(), tx, *streamName, []byte("abc"))
			if err != nil {
				log.Panic("PublishMessage error", zap.Error(err))
			}
			if err := tx.Commit(); err != nil {
				log.Panic("tx commit error", zap.Error(err))
			}
		}
		log.Info("Messages created")
		time.Sleep(time.Second * 1)
	}




}
