test: 
	docker-compose up -d
	RATE=1 testdata/nginx-log-generator >> testdata/nginx.log

lint:
	go fmt ./...
	go test -cover ./...

run:
	rm -rf ./bin
	[[ -f /tmp/hetman.registry.json ]] && truncate -s 0 /tmp/hetman.registry.json 
	mkdir ./bin
	CGO_ENABLED=0 go build -o bin ./cmd/hetman
	./bin/hetman

kill:
	ps aux | grep "hetman" | grep -v grep | awk '{ print $$2 }' | xargs kill -9

reload:
	kill -HUP $$(ps aux | grep "hetman" | grep -v grep | awk '{ print $$2 }')
