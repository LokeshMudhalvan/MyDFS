FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN go build -o dfs cmd/*

FROM scratch
COPY --from=builder /app/dfs /
ENTRYPOINT [ "/dfs" ]
