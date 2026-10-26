FROM golang:1.26.10-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY pkg/ ./pkg

COPY cmd/api ./cmd/api

COPY internal ./internal

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/service ./cmd/api

FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /root/

COPY --from=builder /app/service .

CMD [ "./service" ]