## Builder stage
FROM golang:1.20 as builder
WORKDIR /app
# Allow caching image layer of Go dependencies 
COPY go.mod go.sum .
RUN go mod download
# Build the agent
COPY . .
RUN mkdir -p ./bin /etc/hetman && CGO_ENABLED=0 go build -o ./bin ./... && cp /app/*.yaml /etc/hetman

## Executor stage
FROM scratch
# Move configs to proper directory
COPY --from=builder /etc/hetman /etc/hetman
COPY --from=builder /app/bin/hetman /usr/local/bin/hetman 

ENTRYPOINT ["/usr/local/bin/hetman"]