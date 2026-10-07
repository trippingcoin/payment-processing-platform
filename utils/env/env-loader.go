package env

import (
	"os"

	"github.com/caarlos0/env/v10"
	_ "github.com/caarlos0/env/v10"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog/log"
)

func Load(target interface{}) error {
	if _, err := os.Stat(".env"); err == nil {
		if err := godotenv.Load(); err != nil {
			log.Warn().Err(err).Msg("failed to load .env file")
		} else {
			log.Info().Msg(".env file loaded")
		}
	} else {
		log.Info().Msg(".env file not found, using system env variables...")
	}

	if err := env.Parse(target); err != nil {
		return err
	}
	return nil
}
