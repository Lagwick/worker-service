package section

import "time"

type (
	Client struct {
		Fixer ClientFixer
	}

	ClientFixer struct {
		ApiKey   string        `required:"true" split_words:"true"`
		BaseURL  string        `default:"http://data.fixer.io/api" split_words:"true"`
		CacheTTL time.Duration `default:"30m" split_words:"true"`
	}
)
