package scraper

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"golang.org/x/net/html"
)

func ExtractLinks(rawURL string) ([]string, error) {

	base, _ := url.Parse(rawURL)

	visited := map[string]bool{}
	var links []string

	doc, err := fetchAndParse(rawURL)
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

func fetchAndParse(rawUrl string) (*html.Node, error) {

	res, err := http.Get(rawUrl)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status code error: %d %s", res.StatusCode, res.Status)
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

	return strings.TrimSuffix(resolved.String(), "/"), true
}
