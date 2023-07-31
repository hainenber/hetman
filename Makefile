export OTEL_SERVICE_NAME := "hetman"
export OTEL_EXPORTER_OTLP_PROTOCOL := "http/protobuf"
export OTEL_EXPORTER_OTLP_ENDPOINT := "http://localhost:4318"

vulncheck:
	! command -v govulncheck && go install golang.org/x/vuln/cmd/govulncheck@latest || continue
	govulncheck ./...

lint:
	go fmt ./...
	go clean -testcache
	go test -timeout 2m -cover ./...

run-docker:
	docker-compose -f test/docker-compose.yml up --remove-orphans --build -d 

run:
	docker-compose -f test/docker-compose.yml up victoria-metrics grafana-agent loki grafana --remove-orphans -d
	[[ -d ./bin ]] || mkdir ./bin
	[[ -d ./tmp/tmp_nginx ]] || mkdir -p /tmp/tmp_nginx/
	cp ./test/data/nginx.log /tmp/tmp_nginx/nginx.log
	cp ./test/data/nginx2.log /tmp/tmp_nginx/nginx2.log
	[[ -f /tmp/hetman.registry.json ]] && truncate -s 0 /tmp/hetman.registry.json || continue
	CGO_ENABLED=0 go build -o bin ./cmd/hetman		
	./bin/hetman --mode=agent --config-file=hetman.agent.yaml --log-level=debug

stop:
	docker-compose -f test/docker-compose.yml down -v

dashboard:
	cd deployment/grafana && \
		terraform init && \
		terraform apply -auto-approve

run-aggregator: build
	./bin/hetman --mode=aggregator --aggregator-port=3101 --config-file=hetman.aggregator.yaml

test-aggregator:
	curl -XPOST -H "Content-Type: application/json" -d @test/payload.json http://localhost:3101/logs -v

kill:
	ps aux | grep "hetman" | grep -v grep | awk '{ print $$2 }' | xargs kill -9

reload:
	kill -HUP $$(ps aux | grep "hetman" | grep -v grep | awk '{ print $$2 }')
