package main

import (
	"flag"
	"fmt"
	"log"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/a4eiron/webcrawler/internal/crawler"
)

func main() {

	seedURL := flag.String("seed", "", "seed URL (required)")
	workers := flag.Int("workers", 5, "max crawler workers")
	depth := flag.Int("depth", 10, "max crawling depth")

	flag.Parse()

	if *seedURL == "" {
		fmt.Printf("Usage of %s :\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	c := crawler.New(
		crawler.WithMaxWorkers(*workers),
		crawler.WithMaxDepth(*depth),
		crawler.WithRLCap(10),
		crawler.WithRLRate(2),
	)

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGTERM)

	crawlDone := c.Seed(*seedURL)

	select {
	case <-crawlDone:
		log.Println("Crawl complete", runtime.NumGoroutine())
	case <-done:
		c.Stop()

	}

}
