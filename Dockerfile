FROM devbox:latest

# Project-specific tools
USER root
RUN apk add --no-cache gleam postgresql17-client rebar3 deno
USER dev

WORKDIR /workspace
