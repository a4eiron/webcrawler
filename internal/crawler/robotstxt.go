package crawler

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/temoto/robotstxt"
)

type RobotsCache struct {
	mu    sync.Mutex
	cache map[string]*robotstxt.RobotsData
}

func (rc *RobotsCache) Allowed(domain, path string) bool {
	rc.mu.Lock()
	data, ok := rc.cache[domain]
	rc.mu.Unlock()

	if !ok {
		client := &http.Client{Timeout: 10 * time.Second}
		res, err := client.Get(fmt.Sprintf("https://%s/robots.txt", domain))
		if err != nil {
			return true
		}
		defer res.Body.Close()
		data, err = robotstxt.FromResponse(res)
		if err != nil {
			return true
		}
		rc.mu.Lock()
		rc.cache[domain] = data
		rc.mu.Unlock()
	}

	if data == nil {
		return true
	}
	return data.FindGroup("VCrawler/1.0").Test(path)
}

func NewRobotsCache() *RobotsCache {
	return &RobotsCache{
		cache: make(map[string]*robotstxt.RobotsData),
	}
}
