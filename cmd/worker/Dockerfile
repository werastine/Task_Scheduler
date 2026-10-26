FROM golang:1.26.10-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY pkg/ ./pkg

COPY cmd/worker ./cmd/worker

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/service ./cmd/worker

FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /root/

COPY --from=builder /app/service .

CMD [ "./service" ]