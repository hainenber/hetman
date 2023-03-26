test: 
	docker-compose up -d
	RATE=1 testdata/nginx-log-generator >> testdata/nginx.log

lint:
	go fmt ./...
	go test -v ./...

run:
	go run ./...
