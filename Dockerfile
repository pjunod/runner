# nzbd — multi-stage build: static-ish Rust binary + the PP toolchain.
#
#   docker build -t nzbd \
#     --build-arg NZBD_GIT_DESCRIBE="$(git describe --tags --always --dirty --abbrev=9)" .
#   docker run -d -p 6789:6789 \
#     -v /data/usenet:/data -v ./config:/etc/nzbd nzbd
#
# Or just `make docker-build`, which fills the argument in for you.
#
# Mount the config DIRECTORY, not the file: a file bind mount whose host
# side doesn't exist yet makes Docker create a directory in its place,
# and the first-run setup UI couldn't persist the config it writes.

FROM rust:1.97.1-bookworm AS build
# The build context deliberately excludes `.git` (see .dockerignore), so
# the binary cannot work out its own commit — it has to be told. Without
# this the daemon reports `<version>+unknown`, which is deliberately loud:
# a hundred commits once shipped as an unchanging "v0.1.0" precisely
# because a missing hash failed silently (field report 2026-07-27).
ARG NZBD_GIT_DESCRIBE
ENV NZBD_GIT_DESCRIBE=${NZBD_GIT_DESCRIBE}
WORKDIR /src
COPY . .
RUN cargo build --release -p nzbd

FROM debian:bookworm-slim AS runtime-tools
# The configured defaults require par2, real UnRAR and 7z. Debian's UnRAR
# package is in non-free; unrar-free cannot reliably handle multi-volume RAR.
# Keep both 7z and modern 7zz available for fallback and other archive formats.
RUN sed -i 's/^Components: main$/Components: main non-free/' \
      /etc/apt/sources.list.d/debian.sources \
 && apt-get update \
 && apt-get install -y --no-install-recommends \
      par2 unrar p7zip-full 7zip ca-certificates tini \
 && command -v par2 \
 && command -v unrar \
 && command -v 7z \
 && command -v 7zz \
 && command -v tini \
 && rm -rf /var/lib/apt/lists/*

FROM runtime-tools AS runtime
COPY --from=build /src/target/release/nzbd /usr/local/bin/nzbd

# Unprivileged runtime user; /data is the conventional volume mount.
# /etc/nzbd is nzbd-writable so the first-run setup UI can create the
# config when the container starts without one.
RUN groupadd -g 1000 nzbd \
 && useradd -r -u 1000 -g nzbd -m -d /var/lib/nzbd nzbd \
 && mkdir -p /data /etc/nzbd && chown nzbd:nzbd /data /etc/nzbd /var/lib/nzbd
USER nzbd

# /data is the durable volume, and this names it for the daemon. With no
# config file to read `paths.main_dir` from, an env var is the only way
# boot-time recovery can find the copy of the configuration nzbd keeps
# beside its state — which is what stops a container that lost its config
# mount from coming back up as an unconfigured first-run install.
ENV NZBD_MAIN_DIR=/data
VOLUME ["/data"]
EXPOSE 6789

ENTRYPOINT ["/usr/bin/tini", "--", "nzbd"]
CMD ["run", "--config", "/etc/nzbd/nzbd.toml", "--bind", "0.0.0.0:6789"]
