## Builder stage
FROM golang:1.20 as builder
WORKDIR /app

# Allow caching image layer of Go dependencies 
COPY go.mod go.sum .
RUN go mod download

# Build the agent
COPY . .
RUN mkdir -p ./bin && CGO_ENABLED=0 go build -o ./bin ./...

## Executor stage
FROM alpine:latest

# Move configs to proper directory
RUN mkdir -p /etc/hetman
COPY --from=builder /app/*.yaml /etc/hetman/
COPY --from=builder /app/bin/hetman /usr/local/bin/hetman 