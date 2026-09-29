# Harrier CPU embeddings

vLLM configuration for `microsoft/harrier-oss-v1-0.6b` on `vml.pub` (8 vCPUs,
15 GiB RAM, Ice Lake, no GPU). `pdc-lite.pub` is deferred.

The [Compose file](compose.yaml) pins the model revision, uses FP32, and limits
inputs to 4096 tokens and four concurrent sequences. Query instructions must be
added by clients; documents use plain text. The model returns 1024-dimensional,
L2-normalized embeddings. For reduced dimensions, truncate and normalize again
on the client.

## Deploy

Copy `Dockerfile`, `.dockerignore`, `compose.yaml`, and `test_embeddings.py` to
`/opt/inference/harrier-oss-v1-0.6b` on `vml.pub`. The Hugging Face cache is stored
separately at `/opt/hf_cache` and mounted into the container.

```bash
cd /opt/inference/harrier-oss-v1-0.6b
docker compose up -d --build --wait --wait-timeout 240
python3 test_embeddings.py
docker compose ps
```

The service restarts unless explicitly stopped. Logs rotate at 10 MiB with three
files. `/health` checks engine availability; the embedding tests also exercise
inference, which caught the original failure despite a passing health endpoint.

## Access

The API is published at `http://127.0.0.1:30004` on `vml.pub`. Use an SSH tunnel
from another machine:

```bash
ssh -N -L 30004:127.0.0.1:30004 vml.pub
```

Then run `python3 test_embeddings.py` locally, or send requests:

```bash
curl http://127.0.0.1:30004/v1/embeddings \
  -H 'Content-Type: application/json' \
  -d '{"model":"microsoft/harrier-oss-v1-0.6b","input":"Paris is the capital of France.","encoding_format":"float"}'
```

## CPU compatibility

The pinned upstream image contains vLLM `0.30.0+cpu` and Triton
`3.7.0+git270e696d`. On this host, a single embedding request timed out after
30 seconds with both the default V2 runner and the V1 runner while Triton was
installed. The [Dockerfile](Dockerfile) removes Triton and selects
`VLLM_USE_V2_MODEL_RUNNER=0`, allowing the V1 runner to select its native C++
kernel replacements. No vLLM source files are patched.

The image and model revision are pinned for reproducibility. Re-run the live
tests before upgrading either. This configuration has not been validated on
the deferred 4 GB host.

Verified on 2026-09-28: all seven live tests passed in 0.700 seconds; the original
single-input request returned HTTP 200 in 0.428 seconds. The container was
healthy with zero restarts. Observed container memory was 10.16 GiB, with about
4.0 GiB available on the host. These are smoke-test observations, not load-test
capacity guarantees.

References: [serving research](../docs/agents/research/embeddings/serving.md),
[task](../docs/agents/tasks/embeddings-serv-02-task.md),
[model card](https://huggingface.co/microsoft/harrier-oss-v1-0.6b),
[vLLM CPU documentation](https://docs.vllm.ai/en/latest/getting_started/installation/cpu/).
