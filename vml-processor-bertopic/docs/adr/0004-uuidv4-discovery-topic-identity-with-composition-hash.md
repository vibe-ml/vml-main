# UUIDv4 discovery topic identity with composition hash

Each discovery topic receives a stable UUIDv4 as its primary `discovery_topic_id`. BERTopic's run-local integer IDs are stored alongside in a mapping table for debugging and traceability back to raw BERTopic output but are not used as persistent identifiers. A `composition_hash` column (SHA-256 of sorted member OpenAlex work IDs) enables detecting membership changes across topic runs without comparing full member lists.

UUIDs are globally unique and collision-resistant but less human-readable than sequential integers. Topics are primarily referenced by their generated labels in analyst-facing contexts, so readability of the ID itself is secondary. Split/merge tracking across refits is deferred to a later milestone.

This is an accepted design decision, not implemented behavior. See the [processor spec](../agents/tasks/bertopic/bertopic-spec.md).
