test: 
	docker-compose up -d
	RATE=1 testdata/nginx-log-generator >> testdata/nginx.log

lint:
	go fmt ./...
	go test -v ./...

run:
	go build -o hetman ./cmd/hetman
	./hetman

kill:
	ps aux | grep "hetman" | grep -v grep | awk '{ print $$2 }' | xargs kill

reload:
	kill -HUP $$(ps aux | grep "hetman" | grep -v grep | awk '{ print $$2 }')
