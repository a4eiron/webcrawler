package crawler

import (
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
	c := &Crawler{
		maxWorkers: 5,
		maxDepth:   10,
		jobs:       make(chan Job, 100),
		visited:    map[string]bool{},
		rlCap:      100,
		rlRate:     20,
	}

	for _, opt := range opts {
		opt(c)
	}

	c.ratelimiter = NewTokentBucketRLimiter(c.rlCap, c.rlRate)

	return c
}

func (c *Crawler) Seed(URL string) {
	c.wg.Add(1)
	c.visited[URL] = true
	c.jobs <- Job{Url: URL, Depth: 0}
	c.Process()
}

func (c *Crawler) Process() {
	for range c.maxWorkers {
		go func() {
			for job := range c.jobs {
				c.process(job)
			}
		}()
	}
	c.wg.Wait()
	close(c.jobs)
}

func (c *Crawler) process(job Job) {
	defer c.wg.Done()
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
		if c.visited[link] {
			c.mu.Unlock()
			continue
		}

		c.visited[link] = true
		c.wg.Add(1)
		c.mu.Unlock()
		go func(URL string, depth int) {

			parsedURL, _ := url.Parse(URL)
			if !c.ratelimiter.Allow(parsedURL.Hostname()) {
				// log.Println("not allowed", parsedURL.Hostname())
				time.Sleep(500 * time.Millisecond)
			}
			c.jobs <- Job{Url: URL, Depth: depth}
		}(link, job.Depth+1)
	}
}
