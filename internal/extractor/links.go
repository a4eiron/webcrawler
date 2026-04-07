package extractor

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/net/html"
)

type contentParser struct {
	client *http.Client
}

type LinkExtractor struct {
	cparser *contentParser
}

func (e *LinkExtractor) ExtractLinks(rawURL string) ([]string, error) {

	base, _ := url.Parse(rawURL)

	visited := map[string]bool{}
	var links []string

	doc, err := e.cparser.fetchAndParse(rawURL)
	if err != nil {
		return nil, err
	}

	for node := range doc.Descendants() {
		if node.Type != html.ElementNode || node.Data != "a" {
			continue
		}

		href, found := getAttribute(node, "href")
		if !found {
			continue
		}

		link, ok := resolveURL(base, href)
		if !ok || link == "" {
			continue
		}

		if _, ok := visited[link]; !ok {
			visited[link] = true
			links = append(links, link)
		}
	}

	return links, nil
}

func (cparser *contentParser) fetchAndParse(rawUrl string) (*html.Node, error) {

	res, err := cparser.client.Get(rawUrl)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	ct := res.Header.Get("Content-Type")
	if !strings.Contains(ct, "text/html") {
		return nil, fmt.Errorf("non-html content: %s", ct)
	}

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status error: %s", res.Status)
	}

	doc, err := html.Parse(res.Body)
	if err != nil {
		return nil, err
	}

	return doc, nil

}

func getAttribute(node *html.Node, key string) (string, bool) {
	for _, attr := range node.Attr {
		if attr.Key == key {
			return attr.Val, true
		}
	}
	return "", false
}

func resolveURL(base *url.URL, href string) (string, bool) {
	u, err := url.Parse(href)
	if err != nil {
		return "", false
	}
	resolved := base.ResolveReference(u)
	if resolved.Host != base.Host {
		return "", false
	}
	resolved.Fragment = ""
	resolved.Scheme = "https"

	return strings.TrimSuffix(resolved.String(), "/"), true
}

func New(dial func(ctx context.Context, network, addr string) (net.Conn, error)) *LinkExtractor {
	return &LinkExtractor{
		cparser: &contentParser{
			client: &http.Client{
				Timeout: 10 * time.Second,
				Transport: &http.Transport{
					DialContext: dial,
				}},
		},
	}
}
