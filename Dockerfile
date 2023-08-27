## Builder stage
FROM golang:1.21 as builder

ARG GOPROXY="proxy.golang.org"

WORKDIR /app
# Allow caching image layer of Go dependencies 
COPY go.mod go.sum /app/
RUN go mod download
# Build the agent
COPY . .
RUN mkdir -p ./bin /etc/hetman && \
    CGO_ENABLED=0 go build -o ./bin ./... && \
    stat /app/*.yaml 2>&1 >/dev/null && cp /app/yaml /app/*.yaml /etc/hetman || true

## Executor stage
FROM scratch
# Move configs to proper directory
COPY --from=builder /etc/hetman /etc/hetman
COPY --from=builder /app/bin/hetman /usr/local/bin/hetman 

ENTRYPOINT ["/usr/local/bin/hetman"]