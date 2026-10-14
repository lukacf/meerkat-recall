# Context is a native build.py bundle, never the Recall repository or a live DB.
# Supply a reviewed Linux runtime image by immutable digest; no mutable default.
ARG RUNTIME_IMAGE
FROM ${RUNTIME_IMAGE}
ARG RUNTIME_IMAGE
USER 0:0
RUN case "$RUNTIME_IMAGE" in *@sha256:*) ;; *) exit 1 ;; esac \
    && test "$(dpkg --print-architecture)" = amd64 \
    && . /etc/os-release && test "$VERSION_ID" = 12 \
    && apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3 liblzma5 libstdc++6 libgomp1 zlib1g \
    && rm -rf /var/lib/apt/lists/*
COPY --chmod=0755 recall-surreal /usr/local/bin/recall-surreal
COPY LICENSE.* RECALL_SOURCE_PROVENANCE.json BUILD_PROVENANCE.json CONTAINER_INPUTS.json /usr/share/recall-surreal/
RUN ldd /usr/local/bin/recall-surreal > /usr/share/recall-surreal/runtime-dependencies.txt \
    && ! grep -q 'not found' /usr/share/recall-surreal/runtime-dependencies.txt \
    && dpkg-query -W -f='${binary:Package}\t${Version}\n' > /usr/share/recall-surreal/runtime-packages.txt
# New named volumes inherit this writable data directory for the service UID.
RUN mkdir -p /data && chown 65532:65532 /data
USER 65532:65532
WORKDIR /data
EXPOSE 8000
ENTRYPOINT ["/usr/local/bin/recall-surreal"]
CMD ["--help"]

# Retain the reviewed storage fork repair and its source identity with the image.
COPY surrealkv-0.9.3-backport.patch surrealkv-0.9.3-backport.md surrealkv-0.9.3-upstream-sha256.json /usr/share/recall-surreal/
