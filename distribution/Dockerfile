# Based on https://github.com/open-telemetry/opentelemetry-collector-releases/blob/main/distributions/otelcol-contrib/Dockerfile

FROM --platform=${BUILDPLATFORM:-linux/amd64} alpine:3.22 as certs
RUN apk --update add ca-certificates

FROM scratch

ARG TARGETPLATFORM
ARG BUILDPLATFORM
ARG TARGETOS
ARG TARGETARCH

ARG USER_UID=10001
ARG USER_GID=10001
USER ${USER_UID}:${USER_GID}

COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --chmod=755 ./bin/otelcontribcol_${TARGETOS}_${TARGETARCH} /otelcol-contrib
COPY distribution/config.yaml /etc/otelcol-contrib/config.yaml
ENTRYPOINT ["/otelcol-contrib"]
CMD ["--config", "/etc/otelcol-contrib/config.yaml"]
EXPOSE 4317 4318 55678 55679