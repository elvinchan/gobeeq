package gobeeq

import "time"

type Config struct {
	StallInterval time.Duration
	EnsureScripts bool
	Prefix        string
	Concurrency   uint
	ScriptsProvider
}

var (
	defaultConfig = &Config{
		StallInterval:   time.Microsecond * 5000,
		EnsureScripts:   true,
		Prefix:          "bq",
		Concurrency:     1,
		ScriptsProvider: DefaultScriptsProvider{},
	}
)
