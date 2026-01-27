FROM alpine:latest

RUN apk add --no-cache bash curl jq coreutils ca-certificates

COPY sync.sh /sync.sh
RUN chmod +x /sync.sh

ENTRYPOINT ["/sync.sh"]
