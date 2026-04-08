package urlnorm

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
)

var trackingParams = map[string]struct{}{
	"utm_source":   {},
	"utm_medium":   {},
	"utm_campaign": {},
	"utm_term":     {},
	"utm_content":  {},
	"fbclid":       {},
	"gclid":        {},
}

var blockedExt = map[string]struct{}{
	".zip": {}, ".pdf": {}, ".jpg": {}, ".jpeg": {}, ".png": {}, ".webp": {},
	".mp4": {}, ".mp3": {}, ".avi": {},
}

func Normalize(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}

	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)

	u.Fragment = ""

	if NonHTMLExtension(u.Path) {
		return "", fmt.Errorf("non-html")
	}

	if u.Path != "/" && strings.HasSuffix(u.Path, "/") {
		u.Path = strings.TrimSuffix(u.Path, "/")
	}

	q := u.Query()

	cleanQ := url.Values{}
	for k, vals := range q {
		kLower := strings.ToLower(k)

		if _, skip := trackingParams[kLower]; skip {
			continue
		}

		for _, v := range vals {
			cleanQ.Add(kLower, v)
		}
	}

	keys := make([]string, 0, len(cleanQ))
	for k := range cleanQ {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	sortedQ := url.Values{}
	for _, k := range keys {
		vals := cleanQ[k]
		sort.Strings(vals)
		for _, v := range vals {
			sortedQ.Add(k, v)
		}
	}

	u.RawQuery = sortedQ.Encode()

	return u.String(), nil
}

func NonHTMLExtension(path string) bool {

	for ext := range blockedExt {
		if strings.HasSuffix(path, ext) {
			return true
		}
	}
	return false
}
