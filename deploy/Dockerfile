# Dockerfile
#
# Builds ONE image that holds both programs of this project:
#   /demo   the gateway demo (rate limited API)
#   /rules  the Rules Service
# Which one runs is chosen when the container starts (see docker-compose.yml and the k8s files).
#
# Build from the repository root:
#   docker build -f deploy/Dockerfile -t rate-limiter:local .

# ---- Stage 1: build ----
# A multi-stage build uses a big image to compile, then copies only the result into a tiny final image.
# golang:1.27 has the Go compiler. "AS build" names this stage so stage 2 can copy from it.
FROM golang:1.27 AS build

# All following commands run inside this folder of the build image.
WORKDIR /src

# Copy only the dependency files first. Docker caches this layer, so "go mod download" re-runs only
# when go.mod or go.sum change, not on every code change.
COPY go.mod go.sum ./

# Download the dependencies (go-redis) listed in go.mod.
RUN go mod download

# Copy the source folders. Documentation and deploy files are not needed to compile.
COPY limiter ./limiter
COPY redisstore ./redisstore
COPY rules ./rules
COPY middleware ./middleware
COPY cmd ./cmd

# CGO_ENABLED=0 builds a fully static binary that needs no C libraries, so it runs on the minimal final image.
# -trimpath removes local file paths from the binary. -o writes the result to /out/<name>.
RUN CGO_ENABLED=0 go build -trimpath -o /out/demo ./cmd/demo
RUN CGO_ENABLED=0 go build -trimpath -o /out/rules ./cmd/rules

# ---- Stage 2: final image ----
# distroless "static" has no shell and no package manager, only what a static binary needs.
# That makes the image small and leaves little for an attacker to use. "nonroot" runs as a normal user, not root.
FROM gcr.io/distroless/static-debian13:nonroot

# Copy both compiled programs from the build stage into the final image.
COPY --from=build /out/demo /demo
COPY --from=build /out/rules /rules

# The gateway listens on 8080 and the Rules Service on 9090. EXPOSE documents this; it does not publish the ports.
EXPOSE 8080 9090

# No ENTRYPOINT is set on purpose: each service picks its program, for example "/demo" or "/rules".
