package cache

import (
	"context"
	"log"

	"github.com/redis/go-redis/v9"
)

func RedisClient(ctx context.Context) *redis.Client {

	rClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := rClient.Ping(ctx).Err(); err != nil {
		log.Fatalln("redis is unavailable", err)
	}

	return rClient
}
