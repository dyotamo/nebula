FROM golang:1.26-alpine AS builder
RUN apk add --no-cache git
RUN go install go.k6.io/xk6/cmd/xk6@v1.5.0
RUN mkdir -p /out && xk6 build v1.8.2 --with github.com/grafana/xk6-tcp@v0.2.0 --output /out/k6

FROM alpine:3.22
RUN adduser -D -u 10001 k6
COPY --from=builder /out/k6 /usr/local/bin/k6
COPY perf/cache.js /scripts/cache.js
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/k6"]
