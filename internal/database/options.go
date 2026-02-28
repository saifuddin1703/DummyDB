package database

import (
	"github.com/dummydb/internal/config"
	"github.com/dummydb/internal/storage"
)

// Option is a functional option for configuring a Database
type Option func(*dbOptions)

// dbOptions holds the configuration for a Database
type dbOptions struct {
	config *config.Config
	engine storage.Engine
}

// WithConfig sets the configuration
func WithConfig(cfg *config.Config) Option {
	return func(o *dbOptions) {
		o.config = cfg
	}
}

// WithStorageEngine sets the storage engine
func WithStorageEngine(engine storage.Engine) Option {
	return func(o *dbOptions) {
		o.engine = engine
	}
}
