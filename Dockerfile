# vmctl copies the data. Its version doesn't have to match the clusters: it only streams the native export of
# blue's vmselect into green's vminsert.
ARG VMCTL_VERSION=v1.154.1

FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/vmrebalance ./cmd/vmrebalance

FROM victoriametrics/vmctl:${VMCTL_VERSION} AS vmctl

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=vmctl /vmctl-prod /usr/local/bin/vmctl
COPY --from=build /out/vmrebalance /usr/local/bin/vmrebalance
ENTRYPOINT ["/usr/local/bin/vmrebalance"]
