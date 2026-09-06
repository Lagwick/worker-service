package section

type (
	Repository struct {
		Redis RepositoryRedis
	}

	RepositoryRedis struct {
		Address  string `required:"true" default:"localhost:6379"`
		Password string `default:""`
		DB       int    `default:"0"`
	}
)
