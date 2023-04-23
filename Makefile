test: 
	docker-compose up -d
	RATE=1 testdata/nginx-log-generator >> testdata/nginx.log

lint:
	go fmt ./...
	go test -v ./...

run:
	go run ./...

kill:
	ps aux | grep "go run" | grep -v grep | awk '{ print $2 }' | xargs kill