package main

import (
	"log"
	. "webscraper/internal/parser"
)

func main() {

	if err := Parse("https://rust-lang.org"); err != nil {
		log.Fatal(err)
	}

}
