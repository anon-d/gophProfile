# Build stage
FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o server ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -o worker ./cmd/worker
RUN CGO_ENABLED=0 GOOS=linux go build -o migrator ./cmd/migrator

# Runtime stage
FROM alpine:3.20
RUN apk --no-cache add ca-certificates tzdata
WORKDIR /app
COPY --from=builder /app/server .
COPY --from=builder /app/worker .
COPY --from=builder /app/migrator .
COPY --from=builder /app/web ./web/
COPY --from=builder /app/migrations ./migrations/

EXPOSE 8080