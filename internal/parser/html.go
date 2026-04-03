package parser

import (
	"fmt"
	"net/http"
	"net/url"

	"golang.org/x/net/html"
)

func Parse(rawUrl string) error {

	visited := map[string]bool{}

	res, err := http.Get(rawUrl)
	if err != nil {
		return err
	}

	defer res.Body.Close()

	if res.StatusCode != 200 {
		return fmt.Errorf("status code error: %d %s", res.StatusCode, res.Status)
	}

	doc, err := html.Parse(res.Body)
	if err != nil {
		return err
	}

	base, _ := url.Parse(rawUrl)
	fmt.Println("Base url:", base)

	for n := range doc.Descendants() {
		if n.Type == html.ElementNode && n.Data != "a" {
			continue
		}

		href, found := getAttr(n, "href")
		if !found {
			continue
		}

		resolved := resolvedURL(base, href)

		if _, ok := visited[resolved]; !ok {
			visited[resolved] = true
			fmt.Println(len(visited))
		}
	}

	return nil

}

func getAttr(n *html.Node, key string) (string, bool) {
	for _, attr := range n.Attr {
		if attr.Key == key {
			return attr.Val, true
		}
	}
	return "", false
}

func resolvedURL(base *url.URL, href string) string {
	u, err := url.Parse(href)
	if err != nil {
		return href
	}

	return base.ResolveReference(u).String()
}
