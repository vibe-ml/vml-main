# Keep a single Python 3.14 runtime

CPython 3.14.7 installs and runs the processor's machine-learning stack, including BERTopic, UMAP, HDBSCAN, Numba, llvmlite, scikit-learn, NumPy, the Qdrant client, httpx, SQLAlchemy, and Alembic. The project floor stays `>=3.14`. BERTopic stays in that same runtime rather than moving behind a separate service. The Dagster code location and Docker image therefore use this one Python 3.14 environment: there is no second runtime and no machine-learning service boundary. Lowering the floor, or isolating the fit in its own runtime, was the alternative only if 3.14 failed. It did not.

Verified by [tests/test_ml_runtime_smoke.py](../../tests/test_ml_runtime_smoke.py) and the throwaway migration in [spikes/ml_runtime](../../spikes/ml_runtime/). See the [build instructions](../agents/build.md).
