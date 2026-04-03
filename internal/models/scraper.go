package models

import "sync"

type Scraper struct {
	jobs       chan Job
	visited    map[string]bool
	mu         sync.Mutex
	wg         sync.WaitGroup
	maxWorkers int
	maxDepth   int
}

type Option func(*Scraper)

func WithMaxWorkers(w int) Option {
	return func(s *Scraper) { s.maxWorkers = w }
}

func WithMaxDepth(d int) Option {
	return func(s *Scraper) { s.maxDepth = d }
}

func NewScraper(opts ...Option) *Scraper {
	scraper := &Scraper{
		maxWorkers: 5,
		maxDepth:   10,
	}

	for _, opt := range opts {
		opt(scraper)
	}

	return scraper
}
