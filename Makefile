test: 
	docker-compose up -d
	RATE=1 testdata/nginx-log-generator >> testdata/nginx.log

lint:
	go fmt ./...
	go test -v ./...

run:
	rm -rf ./bin
	truncate -s 0 testdata/hetman.registry.json 
	mkdir ./bin
	go build -o bin ./cmd/hetman
	./bin/hetman

kill:
	ps aux | grep "hetman" | grep -v grep | awk '{ print $$2 }' | xargs kill

reload:
	kill -HUP $$(ps aux | grep "hetman" | grep -v grep | awk '{ print $$2 }')
