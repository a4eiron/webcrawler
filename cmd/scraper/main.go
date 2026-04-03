package main

import (
	"log"

	. "github.com/a4eiron/webscraper/internal/parser"
)

func main() {

	if err := Parse("https://rust-lang.org"); err != nil {
		log.Fatal(err)
	}

}
