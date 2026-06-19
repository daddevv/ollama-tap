FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build -o /ollama-tap ./cmd/ollama-tap

FROM alpine:3.20
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /ollama-tap /usr/local/bin/ollama-tap
ENTRYPOINT ["ollama-tap"]
