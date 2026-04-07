package frontier

import (
	"context"
	"encoding/json"
	"log"
	"time"

	. "github.com/a4eiron/webcrawler/internal/job"
	"github.com/redis/go-redis/v9"
)

type Store struct {
	client     *redis.Client
	queueKey   string
	visitedKey string
}

func (s *Store) Push(ctx context.Context, job Job) {
	encodedJob, err := json.Marshal(job)
	if err != nil {
		log.Println(err)
		return
	}
	s.client.LPush(ctx, s.queueKey, string(encodedJob))
}

func (s *Store) Pop(ctx context.Context) (Job, error) {

	// this mf got me raged so hard
	job, err := s.client.BLPop(ctx, 1*time.Second, s.queueKey).Result()
	if err != nil {
		return Job{}, err
	}

	var decodedJob Job
	err = json.Unmarshal([]byte(job[1]), &decodedJob)

	return decodedJob, err
}

func (s *Store) Seen(ctx context.Context, url string) (bool, error) {
	visited, err := s.client.SAdd(ctx, s.visitedKey, url).Result()
	return visited == int64(0), err
}

func New(rClient *redis.Client) *Store {

	return &Store{
		client:     rClient,
		queueKey:   "crawler:queue",
		visitedKey: "crawler:visited",
	}
}
