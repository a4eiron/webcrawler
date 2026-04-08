package crawler

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/a4eiron/webcrawler/internal/cache"
	"github.com/a4eiron/webcrawler/internal/extractor"
	"github.com/a4eiron/webcrawler/internal/frontier"
	. "github.com/a4eiron/webcrawler/internal/job"
	"github.com/a4eiron/webcrawler/internal/urlnorm"
)

type Crawler struct {
	rdb           *frontier.Store
	linkextractor *extractor.LinkExtractor
	ratelimiter   *TokenBucketRLimiter
	dnsCache      *cache.DNSCache
	robotsCache   *cache.RobotsCache

	maxWorkers int
	maxDepth   int
	rlCap      int
	rlRate     float64

	ctx    context.Context
	cancel context.CancelFunc

	wg       sync.WaitGroup
	workerWg sync.WaitGroup
}

type Option func(*Crawler)

func WithMaxWorkers(w int) Option {
	return func(s *Crawler) { s.maxWorkers = w }
}

func WithMaxDepth(d int) Option {
	return func(s *Crawler) { s.maxDepth = d }
}

func WithRLCap(c int) Option {
	return func(s *Crawler) { s.rlCap = c }
}

func WithRLRate(r float64) Option {
	return func(s *Crawler) { s.rlRate = r }
}

func New(opts ...Option) *Crawler {
	ctx, cancel := context.WithCancel(context.Background())

	c := &Crawler{
		maxWorkers: 5,
		maxDepth:   10,
		rlCap:      100,
		rlRate:     20.0,
		ctx:        ctx,
		cancel:     cancel,
	}

	for _, opt := range opts {
		opt(c)
	}

	rClient := cache.RedisClient(c.ctx)

	c.ratelimiter = NewTokentBucketRLimiter(c.rlCap, c.rlRate)
	c.rdb = frontier.New(rClient)
	c.dnsCache = cache.NewDNSCache(rClient)
	c.robotsCache = cache.NewRobotsCache(c.dnsCache.DialContext)
	c.linkextractor = extractor.New(c.dnsCache.DialContext)

	return c
}

func (c *Crawler) Seed(seedURL string) <-chan struct{} {
	done := make(chan struct{})

	ok, err := c.rdb.PushIfNotSeen(c.ctx, Job{Url: seedURL, Depth: 0})
	if !ok || err != nil {
		close(done)
		return done
	}

	c.wg.Add(1)

	c.Start()

	go func() {
		c.wg.Wait()
		c.cancel()
		c.workerWg.Wait()
		close(done)
	}()

	return done
}

func (c *Crawler) Start() {
	for range c.maxWorkers {
		c.workerWg.Add(1)
		go c.worker()
	}
}

func (c *Crawler) worker() {
	defer c.workerWg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		job, err := c.rdb.Pop(c.ctx)
		if err != nil {
			// log.Println(err)
			if c.ctx.Err() != nil {
				return
			}
			continue
		}

		c.process(job)
	}
}

func (c *Crawler) process(job Job) {
	defer c.wg.Done()

	select {
	case <-c.ctx.Done():
		return
	default:
	}

	u, err := url.Parse(job.Url)
	if err != nil {
		log.Printf("invalid URL %s: %v", job.Url, err)
		return
	}

	for !c.ratelimiter.Allowed(u.Hostname()) {
		select {
		case <-c.ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}

	if !c.robotsCache.Allowed(c.ctx, u.Hostname(), u.Path) {
		return
	}

	fmt.Printf("[Crawl] %s (depth: %d)\n", job.Url, job.Depth)

	links, err := c.linkextractor.ExtractLinks(job.Url)
	if err != nil {
		log.Printf("failed to extract links from %s: %v", job.Url, err)
		return
	}

	if job.Depth >= c.maxDepth {
		return
	}

	for _, link := range links {
		if link == "" {
			continue
		}

		link, err = urlnorm.Normalize(link)
		if err != nil {
			continue
		}

		ok, err := c.rdb.PushIfNotSeen(c.ctx, Job{Url: link, Depth: job.Depth + 1})
		if err != nil {
			log.Println(err)
			continue
		}
		if !ok {
			continue
		}
		c.wg.Add(1)
	}
}

func (c *Crawler) Stop() {
	c.cancel()
}
