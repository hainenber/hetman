export OTEL_SERVICE_NAME := "hetman"
export OTEL_EXPORTER_OTLP_PROTOCOL := "http/protobuf"
export OTEL_EXPORTER_OTLP_ENDPOINT := "http://localhost:4318"

vulncheck:
	! command -v govulncheck && go install golang.org/x/vuln/cmd/govulncheck@latest || continue
	govulncheck ./...

lint:
	go fmt ./...
	go test -cover ./...

run:
	docker-compose -f test/docker-compose.yml up --remove-orphans --build -d 

dashboard:
	cd deployment/grafana && \
		terraform init && \
		terraform apply -auto-approve

run-agent: build
	./bin/hetman --mode=agent --config-file=hetman.agent.yaml

run-aggregator: build
	./bin/hetman --mode=aggregator --aggregator-port=3101 --config-file=hetman.aggregator.yaml

test-aggregator:
	curl -XPOST -H "Content-Type: application/json" -d @test/payload.json http://localhost:3101/logs -v

kill:
	ps aux | grep "hetman" | grep -v grep | awk '{ print $$2 }' | xargs kill -9

reload:
	kill -HUP $$(ps aux | grep "hetman" | grep -v grep | awk '{ print $$2 }')
