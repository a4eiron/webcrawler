package frontier

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type Store struct {
	client     *redis.Client
	queueKey   string
	visitedKey string
}

func (f *Store) CheckAndMarkVisited(ctx context.Context, url string) (bool, error) {
	visited, err := f.client.SAdd(ctx, f.visitedKey, url).Result()
	return visited == int64(0), err
}

func New() *Store {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	return &Store{
		client:     client,
		queueKey:   "crawler:queue",
		visitedKey: "crawler:visited",
	}
}
