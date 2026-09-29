# Single-repetition labeling with representative docs

The processor will generate one candidate headline per discovery topic using BERTopic's representative docs (centroid-nearest works) rather than the WISDOM paper's 15-repetition random-sampling approach. This reduces API cost from approximately 24,000 calls to approximately 1,600 calls for 100 topics while producing a deterministic and coherent label for a single repetition. When multi-candidate generation is added later, sampling will switch to random without replacement following the paper.

The labeling sequence follows WISDOM Algorithm 1 in structure: summarize each selected work individually, concatenate summaries, generate one headline. Per-work sample size is capped at `min(15, topic_size)`. Topics with fewer than 3 works use all works directly. Concatenated summaries that exceed the model's context window are chunked, with a per-chunk headline followed by a final headline.

This is an accepted design decision, not implemented behavior. See the [processor spec](../agents/tasks/bertopic/bertopic-spec.md).
