# Public image for the in-cluster rueidis load generator. Build the static
# binary, stage it next to this Dockerfile as `loadgen`, then run docker build.
FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY loadgen /usr/local/bin/loadgen
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/loadgen"]
