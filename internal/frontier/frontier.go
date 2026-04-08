package frontier

import (
	"context"
	"encoding/json"
	"time"

	. "github.com/a4eiron/webcrawler/internal/job"
	"github.com/redis/go-redis/v9"
)

type Store struct {
	client     *redis.Client
	queueKey   string
	visitedKey string
}

var pushIfNotSeenScript = redis.NewScript(`
local visited = redis.call("SADD", KEYS[1], ARGV[1])
if visited == 1 then
	redis.call("LPUSH", KEYS[2], ARGV[2])
	return 1
end
return 0
`)

func (s *Store) PushIfNotSeen(ctx context.Context, job Job) (bool, error) {
	encoded, err := json.Marshal(job)
	if err != nil {
		return false, err
	}

	res, err := pushIfNotSeenScript.Run(ctx, s.client, []string{s.visitedKey, s.queueKey}, job.Url, string(encoded)).Int()
	if err != nil {
		return false, err
	}

	return res == 1, nil

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

func New(rClient *redis.Client) *Store {

	return &Store{
		client:     rClient,
		queueKey:   "crawler:queue",
		visitedKey: "crawler:visited",
	}
}
