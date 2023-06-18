export OTEL_SERVICE_NAME := "hetman"
export OTEL_EXPORTER_OTLP_PROTOCOL := "http/protobuf"
export OTEL_EXPORTER_OTLP_ENDPOINT := "http://localhost:4318"

vulncheck:
	! command -v govulncheck && go install golang.org/x/vuln/cmd/govulncheck@latest || continue
	govulncheck ./...

lint:
	go fmt ./...
	go test -cover ./...

build:
	docker-compose -f test/docker-compose.yml up -d
	[[ -d ./bin ]] || mkdir ./bin
	[[ -d ./tmp/tmp_nginx ]] || mkdir -p /tmp/tmp_nginx/
	echo '{"a":"1","b":"2"}' > /tmp/tmp_nginx/nginx.log
	echo '{"c":"3","d":"4"}' > /tmp/tmp_nginx/nginx2.log
	[[ -f /tmp/hetman.registry.json ]] && truncate -s 0 /tmp/hetman.registry.json || continue
	CGO_ENABLED=0 go build -o bin ./cmd/hetman

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
