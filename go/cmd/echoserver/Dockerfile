FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o echoserver ./cmd/echoserver

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /app/echoserver /echoserver
ENTRYPOINT ["/echoserver"]
