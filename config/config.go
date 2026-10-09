package config

import "time"

type Config struct {
	Version         string        `env:"APP_VERSION" envDefault:"v0.0.1"`
	Port            int           `env:"APP_PORT" envDefault:"8080"`
	Env             string        `env:"ENV" envDefault:"development"`
	InternalToken   string        `env:"INTERNAL_TOKEN" envDefault:"local-development-token"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"10s"`
	DB              struct {
		DSN             string        `env:"DB_DSN" envDefault:"postgres://"`
		MaxOpenConns    int           `env:"DB_MAX_OPEN_CONNS" envDefault:"25"`
		MinIdleConns    int           `env:"DB_MIN_IDLE_CONNS" envDefault:"2"`
		ConnMaxLifetime time.Duration `env:"DB_CONN_MAX_LIFETIME" envDefault:"30m"`
		ConnMaxIdleTime time.Duration `env:"DB_CONN_MAX_IDLE_TIME" envDefault:"5m"`
	}
}
