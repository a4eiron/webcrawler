package main

import (
	"fmt"
	"log"

	"github.com/a4eiron/webscraper/internal/scraper"
)

func main() {

	links, err := scraper.ExtractLinks("https://go.dev")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(links)

}
