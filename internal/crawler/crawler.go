package crawler

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"
)

type Crawler struct {
	jobs        chan Job
	visited     map[string]bool
	mu          sync.Mutex
	wg          sync.WaitGroup
	maxWorkers  int
	maxDepth    int
	rlCap       int
	rlRate      float64
	ratelimiter *TokenBucketRLimiter
	ctx         context.Context
	cancel      context.CancelFunc
}

type Option func(*Crawler)

func WithMaxWorkers(w int) Option {
	return func(s *Crawler) { s.maxWorkers = w }
}

func WithMaxDepth(d int) Option {
	return func(s *Crawler) { s.maxDepth = d }
}

func WithRLCap(c int) Option {
	return func(s *Crawler) {
		s.rlCap = c
	}
}

func WithRLRate(r float64) Option {
	return func(s *Crawler) { s.rlRate = r }
}

func NewCrawler(opts ...Option) *Crawler {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Crawler{
		maxWorkers: 5,
		maxDepth:   10,
		jobs:       make(chan Job, 100),
		visited:    map[string]bool{},
		rlCap:      100,
		rlRate:     20,
		ctx:        ctx,
		cancel:     cancel,
	}

	for _, opt := range opts {
		opt(c)
	}

	c.ratelimiter = NewTokentBucketRLimiter(c.rlCap, c.rlRate)

	return c
}

func (c *Crawler) Seed(URL string) {
	c.mu.Lock()
	c.visited[URL] = true
	c.mu.Unlock()

	c.wg.Add(1)

	select {
	case c.jobs <- Job{Url: URL, Depth: 0}:
		c.Start()
	case <-c.ctx.Done():
		c.wg.Wait()
		close(c.jobs)
	}

}

func (c *Crawler) Start() {
	defer c.wg.Done()
	for range c.maxWorkers {
		go c.worker()
	}
}

func (c *Crawler) worker() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case job, ok := <-c.jobs:
			if !ok {
				return
			}
			c.process(job)
		}
	}

}

func (c *Crawler) process(job Job) {

	select {
	case <-c.ctx.Done():
		return
	default:
	}

	fmt.Println(job.Url, job.Depth)

	links, err := ExtractLinks(job.Url)
	if err != nil {
		log.Println(err)
	}

	if job.Depth >= c.maxDepth {
		return
	}

	for _, link := range links {

		c.mu.Lock()
		v := c.visited[link]
		c.mu.Unlock()

		if v {
			continue
		}

		c.mu.Lock()
		if c.visited[link] {
			c.mu.Unlock()
			continue
		}
		c.visited[link] = true
		c.mu.Unlock()

		c.wg.Add(1)
		go c.submitJob(link, job.Depth+1)
	}
}

func (c *Crawler) submitJob(link string, depth int) {
	defer c.wg.Done()

	parsedURL, err := url.Parse(link)
	if err != nil {
		log.Println(err)
		return
	}

	for !c.ratelimiter.Allowed(parsedURL.Hostname()) {
		select {
		case <-c.ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}

	select {
	case c.jobs <- Job{Url: link, Depth: depth}:
		return
	case <-c.ctx.Done():
		return
	}

}

func (c *Crawler) Stop() {
	c.cancel()
}
