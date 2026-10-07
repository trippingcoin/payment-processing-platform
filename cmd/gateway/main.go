package main

import (
	"triple-p/config"
	"triple-p/utils/env"
)

func main() {
	var cfg config.Config
	if err := env.Load(&cfg); err != nil {
		panic(err)
	}

}
