# Crawler documentation snapshot

Reference documentation for processing, embedding, and topic modeling of data collected by `vml-crawler-openalex`.

Copied from `~/vibeml/vml-crawler-openalex` on 2026-09-28. Original contents and directory layout are preserved. The [manifest](manifest.json) records the source commit, working-tree status, file checksums, and references outside this snapshot. Linked background research was copied as a regular file so this snapshot does not depend on a local symlink.

Start with the embedding brief and BERTopic report, then the database schema, abstract examples, and current-version selection contract. Research recommendations and unresolved assumptions remain proposals; copying them does not select a model or establish processor configuration.

Paths, commands, dependency requirements, and completed-task claims inside copied documents refer to the crawler. References to its source code, deployment files, and omitted documentation must be resolved in the source checkout. Some background references are already missing there; the manifest distinguishes these. Crawler agent instructions, credentials, source code, and unrelated operational task history are not included.

## Embedding and topic modeling

- [embed-01-task](docs/agents/tasks/embed-01-task.md)
- [BERTopic for OpenAlex works](docs/agents/research/bertopic-openalex.md)
- [From scientific abstracts to technology candidates](docs/agents/research/technologies/abstract-parsing.md)
- [technologies-01-task](docs/agents/tasks/technologies-01-task.md)

## Reading the collected data

- [PostgreSQL Database Schema & Model Architecture](docs/architecture/db.md)
- [OpenAlex Abstract Ingestion & Reconstruction Samples](docs/ingestion-abstract.md)
- [Retrieve a work from VML raw data](docs/raw-data-get-work.md)
- [Large Language Diffusion Models — W4407679583](docs/agents/research/technologies/work-w4407679583.codex.md)
- [OpenAlex architecture](docs/openalex.md)
- [OpenAlex deployment on vml](docs/deploy/openalex-deployment-vml.md)

## Collection scope, versions, and derived outputs

- [OpenAlex Crawler](README.md)
- [OpenAlex ingestion](docs/ingestion.md)
- [Investigate OpenAlex collection for the hackathon MVP](docs/agents/tasks/fetch-agreement.md)
- [OpenAlex ingestion](docs/agents/tasks/ingestion/spec.md)
- [Bounded snapshot sample](docs/agents/tasks/ingestion/snapshot-sample.md)
- [API partition source mapping](docs/agents/tasks/ingestion/api-source-mapping.md)
- [06: Reconcile observed corrections and current selections](docs/agents/tasks/ingestion/issues/06-reconcile-work-history.md)
- [08: Rebuild derived outputs without recollection](docs/agents/tasks/ingestion/issues/08-rebuild-derived-outputs.md)
- [OpenAlex ELT: collection, checkpoints, and storage](docs/agents/research/data-openalex-elt.md)

## Domain context and analytical requirements

- [Emerging Technology Intelligence](CONTEXT.md)
- [Minimal OpenAlex data for the MVP](docs/agents/research/data-openalex.md)
- [OpenAlex Data Metrics and Weak Signal Capabilities](docs/agents/research/metrics.md)
- [Сервис выявления зарождающихся научно-технологических трендов](docs/agents/research/trends-01-research.md)
