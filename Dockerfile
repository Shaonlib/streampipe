FROM golang:1.23-alpine AS builder

RUN apk add --no-cache git

WORKDIR /app

COPY . .
RUN GONOSUMCHECK=* GOFLAGS=-mod=mod go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /streampipe .

FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app
COPY --from=builder /streampipe .
COPY config.yaml .

EXPOSE 8080

ENTRYPOINT ["/app/streampipe", "-config", "/app/config.yaml"]
