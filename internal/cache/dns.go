package cache

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/redis/go-redis/v9"
)

type DNSCache struct {
	client *redis.Client
}

func (d *DNSCache) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if d == nil {
		return nil, fmt.Errorf("DNSCache is nill")
	}
	host, port, _ := net.SplitHostPort(addr)

	ip, err := d.client.Get(ctx, host).Result()
	if err != nil || ip == "" {

		addrs, err := net.DefaultResolver.LookupHost(ctx, host)
		if err != nil {
			return nil, err
		}

		ip = addrs[0]
		d.client.Set(ctx, host, ip, 5*time.Minute)
		log.Println(ip)
	}

	return (&net.Dialer{}).DialContext(ctx, network, net.JoinHostPort(ip, port))

}

func NewDNSCache(rClient *redis.Client) *DNSCache {
	return &DNSCache{client: rClient}
}
