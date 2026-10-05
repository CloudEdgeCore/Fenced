# Minimal agent runtime image for gVisor/OCI isolation drill.
# Build: docker build -t fenced-runtime:latest -f deploy/oci/agent-runtime/Dockerfile .
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /agent-runtime ./deploy/oci/agent-runtime/

FROM scratch
COPY --from=build /agent-runtime /agent-runtime
ENTRYPOINT ["/agent-runtime"]