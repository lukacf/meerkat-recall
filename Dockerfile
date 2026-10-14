# syntax=docker/dockerfile:1@sha256:7b32114e84ca21aeb2d6f871a0b62b3a0762193f75d9afd048ee4cbe889f3148
# Build with scripts/release/build-container from the exact committed source.
ARG BUILDER_IMAGE=rust@sha256:4673f78db88b71f09d5451bbc404734807918161241215ba0a50bbbe9b448117
ARG RUNTIME_IMAGE=debian@sha256:a4672c0cb26fbdde88e38fa2dfb6c681942306680e41e4378b28770b6e79ee91
FROM ${BUILDER_IMAGE} AS builder
WORKDIR /app
ARG BUILDER_IMAGE
ARG RUNTIME_IMAGE
ARG SOURCE_COMMIT=unversioned-development
ARG SOURCE_TREE=unversioned-development
ARG SOURCE_SHA256=unversioned-development
ARG CARGO_BUILD_JOBS=8
ARG RECALL_BUILD_CACHE_ID=recall-unqualified-development
ENV CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 BINARYEN_CORES=8 TLC_WORKERS=8 LZMA_API_STATIC=1
RUN case "$BUILDER_IMAGE,$RUNTIME_IMAGE" in *@sha256:*,*@sha256:*) ;; *) exit 1 ;; esac \
    && test "$(dpkg --print-architecture)" = amd64 \
    && . /etc/os-release && test "$VERSION_ID" = 12 \
    && apt-get update && apt-get install -y --no-install-recommends \
    clang libclang-dev cmake make g++ pkg-config libssl-dev liblzma-dev python3 \
    && rm -rf /var/lib/apt/lists/* \
    && dpkg-query -W -f='${binary:Package}\t${Version}\n' > /build-packages.unsorted \
    && LC_ALL=C sort /build-packages.unsorted > /build-packages.txt \
    && rm /build-packages.unsorted
COPY Cargo.toml Cargo.lock ./
COPY vendor/ vendor/
COPY crates/ crates/
COPY bin/ bin/
COPY skills/ skills/
COPY tests/ tests/
COPY xtask/ xtask/
COPY scripts/repo-cargo scripts/repo-cargo
COPY scripts/release/artifact.py scripts/release/static_ort_receipt.py scripts/release/linux_static_ort.py scripts/release/linux-static-ort.lock.json scripts/release/
COPY scripts/release/onnxruntime-notices/ scripts/release/onnxruntime-notices/
COPY docs/architecture/living-wiki.mdx docs/architecture/living-wiki.mdx
RUN python3 vendor/verify_recall_surrealdb.py
RUN --mount=type=cache,id=${RECALL_BUILD_CACHE_ID},target=/root/.cache/rust-workspaces,sharing=locked \
    test -n "$SOURCE_COMMIT" && test -n "$SOURCE_TREE" && test -n "$SOURCE_SHA256" \
    && test "$CARGO_BUILD_JOBS" -ge 1 && test "$CARGO_BUILD_JOBS" -le 16 \
    && test -z "${ORT_LIB_PATH:-}${ORT_LIB_LOCATION:-}${ORT_PREFER_DYNAMIC_LINK:-}${ORT_IOS_XCFWK_PATH:-}${ORT_IOS_XCFWK_LOCATION:-}" \
    && mkdir /out \
    && ./scripts/repo-cargo --print-env > /out/cargo-env \
    && ORT_LIB_PATH="$(python3 scripts/release/linux_static_ort.py --cargo-env /out/cargo-env)" \
    && export ORT_LIB_PATH ORT_PREFER_DYNAMIC_LINK=0 \
    && chrt --idle 0 ionice -c3 ./scripts/repo-cargo build --locked --profile deploy -p meerkat-recall-bin --features rocksdb,fastembed \
    && python3 scripts/release/static_ort_receipt.py --cargo-env /out/cargo-env --target x86_64-unknown-linux-gnu --output /out/static-ort.json \
    && cp "$(sed -n 's/^CARGO_TARGET_DIR=//p' /out/cargo-env)/deploy/meerkat-recall" /out/meerkat-recall
RUN python3 scripts/release/artifact.py runtime-dependencies --binary /out/meerkat-recall --target x86_64-unknown-linux-gnu > /out/runtime-dependencies.json
RUN python3 scripts/release/artifact.py reference-source-receipt > /out/reference-source.json
RUN SOURCE_COMMIT="$SOURCE_COMMIT" SOURCE_TREE="$SOURCE_TREE" SOURCE_SHA256="$SOURCE_SHA256" BUILDER_IMAGE="$BUILDER_IMAGE" RUNTIME_IMAGE="$RUNTIME_IMAGE" chrt --idle 0 ionice -c3 python3 - <<'PY'
import hashlib, json, os, pathlib, subprocess, tomllib

def sha(path):
    with open(path, 'rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()
manifest = {
    'format': 'meerkat-recall-container/v1',
    'version': tomllib.loads(pathlib.Path('Cargo.toml').read_text())['workspace']['package']['version'],
    'source_commit': os.environ['SOURCE_COMMIT'], 'source_tree': os.environ['SOURCE_TREE'],
    'source_tar_sha256': os.environ['SOURCE_SHA256'], 'cargo_lock_sha256': sha('Cargo.lock'),
    'binary_sha256': sha('/out/meerkat-recall'),
    'rustc': subprocess.check_output(['rustc', '--version'], text=True).strip(),
    'version_output': subprocess.check_output(['/out/meerkat-recall', '--version'], text=True).strip(),
    'build_command': './scripts/repo-cargo build --locked --profile deploy -p meerkat-recall-bin --features rocksdb,fastembed',
    'build_profile': 'deploy', 'features': ['rocksdb', 'fastembed'], 'default_features': True,
    'build_resources': {'cache_id': os.environ['RECALL_BUILD_CACHE_ID'], 'cargo_jobs': int(os.environ['CARGO_BUILD_JOBS']), 'tlc_workers': int(os.environ['TLC_WORKERS'])},
    'runtime_dependencies': json.loads(pathlib.Path('/out/runtime-dependencies.json').read_text()),
    'static_onnxruntime': json.loads(pathlib.Path('/out/static-ort.json').read_text()),
    'container_inputs': {'platform': 'linux/amd64', 'builder_image': os.environ['BUILDER_IMAGE'], 'runtime_image': os.environ['RUNTIME_IMAGE']},
    'builder_packages': pathlib.Path('/build-packages.txt').read_text(),
    'native_reference_capability': json.loads(pathlib.Path('/out/reference-source.json').read_text()),
    'component_licenses': {'surrealdb-core-2.6.5': 'BUSL-1.1', 'surrealdb-2.6.5': 'BUSL-1.1'},
    'model_assets': 'separate persistent cache; not included in this image',
    'qualification': 'not established by image build; requires exact-image acceptance receipt',
}
pathlib.Path('/out/provenance.json').write_text(json.dumps(manifest, indent=2) + '\n')
PY

FROM ${RUNTIME_IMAGE} AS runtime
ARG RUNTIME_IMAGE
ARG SOURCE_COMMIT=unversioned-development
ARG SOURCE_TREE=unversioned-development
ARG SOURCE_SHA256=unversioned-development
LABEL org.opencontainers.image.title="Meerkat Recall" \
      org.opencontainers.image.source="https://github.com/lukacf/meerkat-recall" \
      org.opencontainers.image.revision="$SOURCE_COMMIT" \
      org.meerkat.recall.source-tree="$SOURCE_TREE" \
      org.meerkat.recall.source-tar-sha256="$SOURCE_SHA256"
RUN case "$RUNTIME_IMAGE" in *@sha256:*) ;; *) exit 1 ;; esac \
    && test "$(dpkg --print-architecture)" = amd64 \
    && . /etc/os-release && test "$VERSION_ID" = 12 \
    && apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 liblzma5 libstdc++6 libgomp1 curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 recall \
    && useradd --uid 10001 --gid 10001 --home-dir /var/lib/meerkat-recall --shell /usr/sbin/nologin recall \
    && mkdir -p /var/lib/meerkat-recall/data /var/lib/meerkat-recall/recovery /var/lib/meerkat-recall/backups /var/lib/meerkat-recall/model-cache /var/lib/meerkat-recall/model-assets \
    && chown -R 10001:10001 /var/lib/meerkat-recall
COPY --from=builder /out/meerkat-recall /usr/local/bin/meerkat-recall
RUN ln -s meerkat-recall /usr/local/bin/elephant
COPY --from=builder /out/provenance.json /usr/share/meerkat-recall/provenance.json
RUN ldd /usr/local/bin/meerkat-recall > /usr/share/meerkat-recall/runtime-dependencies.txt \
    && ! grep -q 'not found' /usr/share/meerkat-recall/runtime-dependencies.txt \
    && dpkg-query -W -f='${binary:Package}\t${Version}\n' > /tmp/runtime-packages.unsorted \
    && LC_ALL=C sort /tmp/runtime-packages.unsorted > /usr/share/meerkat-recall/runtime-packages.txt \
    && rm /tmp/runtime-packages.unsorted
COPY --from=builder /app/skills /usr/share/meerkat-recall/skills
COPY LICENSE LICENSE-MIT LICENSE-APACHE THIRD_PARTY_NOTICES.md /usr/share/licenses/meerkat-recall/
COPY scripts/release/onnxruntime-notices/ /usr/share/licenses/meerkat-recall/onnxruntime-1.24.2/
COPY scripts/release/meerkat-notices/ /usr/share/licenses/meerkat-recall/meerkat-0.9.0/
COPY scripts/release/provision-fastembed /usr/share/meerkat-recall/provision-fastembed
COPY scripts/release/ARTIFACT_README.md /usr/share/meerkat-recall/README.md
COPY scripts/release/README.md scripts/release/DARWIN_BUILDER.md /usr/share/meerkat-recall/scripts/release/
COPY docs/internal/recall-preview-contract /usr/share/meerkat-recall/docs/internal/recall-preview-contract
COPY docs/snapshot-durability.md docs/embedded-durable-commits.md /usr/share/meerkat-recall/docs/
COPY scripts/release/THIRD_PARTY_SOURCE_NOTICES.txt /usr/share/meerkat-recall/third-party/SOURCE_NOTICES.txt
COPY vendor/meerkat-provider-upstream.json /usr/share/meerkat-recall/third-party/meerkat-provider-upstream.json
COPY vendor/tokenizers-0.22.2 /usr/share/meerkat-recall/third-party/tokenizers-0.22.2
COPY vendor/rav1e-0.8.1 /usr/share/meerkat-recall/third-party/rav1e-0.8.1
COPY vendor/surrealdb-core-2.6.5 /usr/share/meerkat-recall/third-party/surrealdb-core-2.6.5
COPY vendor/surrealdb-2.6.5 /usr/share/meerkat-recall/third-party/surrealdb-2.6.5
COPY vendor/surrealdb-core-2.6.5-upstream-sha256.json vendor/surrealdb-2.6.5-upstream-sha256.json vendor/RECALL_SURREALDB_PATCH.md /usr/share/meerkat-recall/third-party/
COPY vendor/surrealdb-core-2.6.5/LICENSE /usr/share/licenses/meerkat-recall/surrealdb-core-2.6.5-LICENSE
COPY vendor/surrealdb-2.6.5/LICENSE /usr/share/licenses/meerkat-recall/surrealdb-2.6.5-LICENSE
COPY vendor/pdf-inspector /usr/share/meerkat-recall/third-party/pdf-inspector
COPY vendor/surrealkv-0.9.3 /usr/share/meerkat-recall/third-party/surrealkv-0.9.3
COPY vendor/surrealkv-0.9.3-backport.md vendor/surrealkv-0.9.3-backport.patch vendor/surrealkv-0.9.3-upstream-sha256.json /usr/share/meerkat-recall/third-party/
WORKDIR /var/lib/meerkat-recall
USER 10001:10001
ENV RUST_LOG=info ELEPHANT_HOST=0.0.0.0 ELEPHANT_PORT=3000 \
    PIPELINE_SKILLS_DIR=/usr/share/meerkat-recall/skills \
    FASTEMBED_CACHE_DIR=/var/lib/meerkat-recall/model-cache
EXPOSE 3000
STOPSIGNAL SIGTERM
# Liveness only. Authenticated readiness belongs in the service-manager probe.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl --fail --silent --max-time 4 http://127.0.0.1:3000/v1/health > /dev/null || exit 1
CMD ["meerkat-recall", "serve", "--shutdown-timeout-seconds", "60"]
