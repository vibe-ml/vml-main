# vLLM 0.30.0 CPU; pin the tested image, including its Python dependencies.
FROM vllm/vllm-openai-cpu@sha256:85d126740e78e9b7270b6cfc01159776d066c445609b7167faf32efc4a86f19d

# The bundled Triton CPU JIT hangs on vml.pub. Without Triton, the V1 CPU
# runner selects vLLM's native C++ kernel replacements.
RUN uv pip uninstall --python /opt/venv/bin/python triton
ENV VLLM_USE_V2_MODEL_RUNNER=0
