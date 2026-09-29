# Scientific Topic Discovery

A glossary for discovering research themes in scientific works and describing them for analysis. Upstream evidence and weak-signal terminology follows the [crawler glossary](docs/upstream/vml-crawler-openalex/CONTEXT.md).

## Language

**Discovery topic (WISDOM/BERTopic topic)**:
A research theme discovered within a selected corpus, represented by a cluster of semantically related scientific works and representative keywords. It is distinct from an OpenAlex topic and does not by itself establish a technology candidate or weak signal.
_Avoid_: OpenAlex topic, technology candidate, or weak signal as synonyms; unqualified “topic” when both topic systems are discussed.

**OpenAlex topic**:
A research classification supplied by OpenAlex, belonging to its hierarchy of subfields, fields, and domains. It describes upstream classification rather than a discovery topic produced from the processor's selected corpus.
_Avoid_: Discovery topic as a synonym.

The discovery-topic definition follows the [WISDOM paper](<docs/agents/research/wisdom/WISDOM: An AI-powered framework for emerging research detection using weak signal analysis and advanced topic modelling.pdf>), §3.2.1 and Algorithm 1, pp. 4–7.

**Topic run**:
A discovery analysis of a fixed corpus, with its resulting discovery topics, member works, and representative keywords. Separate runs represent separate analyses, even when they examine the same works.

**Discovery topic label**:
A generated, human-readable headline describing a discovery topic's research theme. It does not establish technology-candidate status.
_Avoid_: Using a generated label as primary evidence or a technology-candidate name without review.

**Emergence observation**:
The measured publication share of one discovery topic across the periods of one topic run, together with its monthly share change and, when two complete calendar years are available, its annual growth and emergence quadrant. It is an emergence indicator for that topic.
_Avoid_: Weak signal, technology candidate, eligibility

**Emergence quadrant**:
The class of an emergence observation that has two complete calendar years: WISDOM weak, WISDOM strong, latent, or not strong but well-known.
_Avoid_: Weak signal, insufficient historical coverage, newly observed

**Insufficient historical coverage**:
The status of an emergence observation whose topic run contains fewer than two complete calendar years. The annual quadrant is withheld.
_Avoid_: Newly observed, latent, WISDOM weak

**Newly observed**:
A discovery topic with a usable multi-year window and no valid adjacent growth pair. Its stored annual growth is -1.
_Avoid_: Insufficient historical coverage, latent

**Monthly share change**:
The change in a discovery topic's publication share from month to month inside one topic run. It is a monitoring measure.
_Avoid_: Annual growth, emergence quadrant

**Work summary**:
A generated concise account of a single work's title and abstract. It is not a primary source.

**Composition hash**:
A fingerprint of the set of works belonging to a discovery topic, used to compare topic membership. Matching membership alone does not establish unchanged source text or continuity of a research theme.
