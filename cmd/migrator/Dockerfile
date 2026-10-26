FROM golang:1.26.10-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY cmd/migrator ./cmd/migrator

COPY pkg/scheduler ./pkg/scheduler

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/service ./cmd/migrator

FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /root/

COPY --from=builder /app/service .

CMD [ "./service" ]