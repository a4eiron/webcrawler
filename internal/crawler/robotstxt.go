package crawler

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/temoto/robotstxt"
)

type RobotsCache struct {
	mu     sync.RWMutex
	cache  map[string]*robotstxt.RobotsData
	client *http.Client
}

func (rc *RobotsCache) Allowed(ctx context.Context, domain, path string) bool {
	rc.mu.RLock()
	data, ok := rc.cache[domain]
	rc.mu.RUnlock()

	if !ok {
		req, err := http.NewRequestWithContext(ctx, "GET",
			fmt.Sprintf("https://%s/robots.txt", domain), nil)

		if err != nil {
			log.Println(err)
			return false
		}
		res, err := rc.client.Do(req)
		if err != nil {
			log.Println(err)
			return false
		}
		defer res.Body.Close()

		// if unreachable, disallow -> RFC 9309
		if res.StatusCode >= 500 {
			return false
		}

		// if unavailable, allow -> RFC 9309
		if res.StatusCode >= 400 {
			rc.mu.Lock()
			rc.cache[domain] = nil
			rc.mu.Unlock()
			return true
		}

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

	group := data.FindGroup("VCrawler/1.0")
	if group == nil {
		group = data.FindGroup("*")
	}

	if group == nil {
		return true
	}

	return group.Test(path)
}

func NewRobotsCache() *RobotsCache {
	return &RobotsCache{
		cache:  make(map[string]*robotstxt.RobotsData),
		client: &http.Client{Timeout: 10 * time.Second},
	}
}
