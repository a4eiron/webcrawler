package crawler

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/a4eiron/webcrawler/internal/extractor"
	"github.com/a4eiron/webcrawler/internal/frontier"
)

type Crawler struct {
	jobs chan Job
	// visited     map[string]bool
	// mu          sync.Mutex

	rdb           *frontier.Store
	linkextractor *extractor.LinkExtractor
	wg            sync.WaitGroup // tracks jobs
	workerWg      sync.WaitGroup // tracks workers
	maxWorkers    int
	maxDepth      int
	rlCap         int
	rlRate        float64
	ratelimiter   *TokenBucketRLimiter
	robotsCache   *RobotsCache
	ctx           context.Context
	cancel        context.CancelFunc
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

func New(opts ...Option) *Crawler {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Crawler{
		maxWorkers: 5,
		maxDepth:   10,
		jobs:       make(chan Job, 100),
		// visited:    map[string]bool{},
		rdb:           frontier.New(),
		linkextractor: extractor.New(),
		rlCap:         100,
		rlRate:        20,
		ctx:           ctx,
		cancel:        cancel,
	}

	for _, opt := range opts {
		opt(c)
	}

	c.ratelimiter = NewTokentBucketRLimiter(c.rlCap, c.rlRate)
	c.robotsCache = NewRobotsCache()

	return c
}

func (c *Crawler) Seed(URL string) <-chan struct{} {

	done := make(chan struct{})
	// c.mu.Lock()
	// c.visited[URL] = true
	// c.mu.Unlock()

	c.rdb.CheckAndMarkVisited(c.ctx, URL)

	c.Start()

	c.wg.Add(1)
	c.jobs <- Job{Url: URL, Depth: 0}

	go func() {
		c.wg.Wait()
		close(c.jobs)
		c.cancel()
		c.workerWg.Wait()
		close(done)
	}()

	return done
}

func (c *Crawler) Start() {
	for range c.maxWorkers {
		c.workerWg.Go(func() {
			c.worker()
		})
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
	defer c.wg.Done()

	select {
	case <-c.ctx.Done():
		return
	default:
	}

	url, err := url.Parse(job.Url)
	if err != nil {
		return
	}

	if !c.robotsCache.Allowed(c.ctx, url.Hostname(), url.Path) {
		return
	}

	fmt.Println(job.Url, job.Depth)

	links, err := c.linkextractor.ExtractLinks(job.Url)
	if err != nil {
		log.Println(err)
	}

	if job.Depth >= c.maxDepth {
		return
	}

	for _, link := range links {
		// c.mu.Lock()
		// if c.visited[link] {
		// 	c.mu.Unlock()
		// 	continue
		// }
		// c.visited[link] = true
		// c.mu.Unlock()

		visited, err := c.rdb.CheckAndMarkVisited(c.ctx, link)
		if visited {
			if err != nil {
				log.Println(err)
			}
			continue
		}

		c.wg.Add(1)
		go c.submitJob(link, job.Depth+1)
	}
}

func (c *Crawler) submitJob(link string, depth int) {

	parsedURL, err := url.Parse(link)
	if err != nil {
		log.Println(err)
		c.wg.Done()
		return
	}

	for !c.ratelimiter.Allowed(parsedURL.Hostname()) {
		select {
		case <-c.ctx.Done():
			c.wg.Done()
			return
		case <-time.After(100 * time.Millisecond):
		}
	}

	select {
	case c.jobs <- Job{Url: link, Depth: depth}:
		return
	case <-c.ctx.Done():
		c.wg.Done()
		return
	}

}

func (c *Crawler) Stop() {
	c.cancel()
}
