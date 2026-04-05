package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	. "github.com/a4eiron/webcrawler/internal/crawler"
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

	c := NewCrawler(WithMaxWorkers(*workers), WithMaxDepth(*depth), WithRLCap(10), WithRLRate(2))
	go func() {
		c.Seed(*seedURL)
	}()

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGTERM)

	<-done
	fmt.Println("shutting down...")
	c.Stop()

}
