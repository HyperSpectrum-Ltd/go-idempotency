module github.com/HyperSpectrum-Ltd/go-idempotency

go 1.25.0

require (
	github.com/google/uuid v1.6.0
	github.com/stretchr/testify v1.11.1
	go.opentelemetry.io/otel v1.32.0
	go.opentelemetry.io/otel/trace v1.32.0
	go.uber.org/zap v1.27.0
)

require (
	github.com/HyperSpectrum-Ltd/go-domainkit v0.0.0
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.opentelemetry.io/otel/metric v1.32.0 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/HyperSpectrum-Ltd/go-domainkit => ../go-domainkit
