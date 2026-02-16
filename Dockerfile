FROM devbox:latest

# Project-specific tools
USER root
RUN apk add --no-cache gleam
USER dev

WORKDIR /workspace
