ARG GO_BUILD_IMAGE=golang:1.26.6-bookworm@sha256:116d58cbd88c1297624acc6e967a060012422bacf9930927e23fb719189c6f36
FROM --platform=linux/amd64 ${GO_BUILD_IMAGE} AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
       -trimpath -buildvcs=false \
       -ldflags="-s -w" \
       -o /out/mahoroba ./cmd/mahoroba
RUN chmod 0555 /out/mahoroba \
    && mkdir -p /out/rootfs/etc/ssl/certs \
                /out/rootfs/etc/mahoroba \
                /out/rootfs/licenses \
                /out/rootfs/var/lib/mahoroba \
                /out/rootfs/var/lib/mahoroba-files \
                /out/rootfs/run/secrets \
    && cp /etc/ssl/certs/ca-certificates.crt /out/rootfs/etc/ssl/certs/ca-certificates.crt \
    && cp /src/internal/releaseasset/THIRD_PARTY_LICENSES.txt /out/rootfs/licenses/THIRD_PARTY_LICENSES.txt \
    && chmod 0555 /out/rootfs/etc /out/rootfs/etc/ssl /out/rootfs/etc/ssl/certs /out/rootfs/licenses \
    && chmod 0444 /out/rootfs/etc/ssl/certs/ca-certificates.crt /out/rootfs/licenses/THIRD_PARTY_LICENSES.txt \
    && chmod 0755 /out/rootfs/etc/mahoroba \
    && chmod 0700 /out/rootfs/var /out/rootfs/var/lib /out/rootfs/var/lib/mahoroba \
                  /out/rootfs/var/lib/mahoroba-files \
                  /out/rootfs/run /out/rootfs/run/secrets

FROM scratch

COPY --from=build --chown=0:0 --chmod=0555 /out/mahoroba /mahoroba
COPY --from=build --chown=0:0 /out/rootfs/etc /etc
COPY --from=build --chown=0:0 /out/rootfs/licenses /licenses
COPY --chown=0:0 --chmod=0444 LICENSE COPYRIGHT /licenses/
COPY --chown=0:0 --chmod=0444 internal/httpui/THIRD_PARTY_NOTICES.md /licenses/WEB_UI_THIRD_PARTY_NOTICES.md
COPY --from=build --chown=65532:65532 /out/rootfs/var /var
COPY --from=build --chown=65532:65532 /out/rootfs/run /run
COPY --chown=0:0 --chmod=0444 docker/config.toml /etc/mahoroba/config.toml

ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV MAHOROBA_PROVIDER_ALLOW_DOCKER_HOST_HTTP=true
VOLUME ["/var/lib/mahoroba"]
VOLUME ["/var/lib/mahoroba-files"]
USER 65532:65532
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD ["/mahoroba","admin","healthcheck"]
ENTRYPOINT ["/mahoroba"]
CMD ["admin","serve","--config","/etc/mahoroba/config.toml","--data-dir","/var/lib/mahoroba/data","--listen","0.0.0.0:8788","--container-listen"]
