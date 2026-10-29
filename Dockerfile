FROM golang:1.27.3-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG COMMIT=unknown
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" -o /nai-relay ./cmd/nai-relay \
    && mkdir -p /runtime/data /runtime/tmp && chmod 1777 /runtime/tmp

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /nai-relay /nai-relay
COPY --from=build --chown=65532:65532 /runtime/ /
USER 65532:65532
ENV NAI_RELAY_DATABASE=/data/nai-relay.db
EXPOSE 8080
VOLUME ["/data"]
ENTRYPOINT ["/nai-relay"]
