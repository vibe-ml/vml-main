# Preserve outliers and exclude from TEM

The processor preserves BERTopic outliers (topic `-1`) without automatic reassignment. Topic `-1` receives no emergence observation and is excluded from the median. Outlier works stay in each period's share denominator; that correction is [ADR 0006](0006-include-outliers-in-the-share-denominator.md). Outliers remain available for later inspection and possible reassignment in a future refit. Aggressive outlier reassignment risks hiding early themes that have not yet formed coherent clusters. Keeping them in `-1` preserves this evidence at the cost of a potentially large outlier population that needs separate monitoring.

Preserving topic `-1` is implemented topic-run behavior. Emergence observations are not implemented yet. See the [processor spec](../agents/tasks/bertopic/bertopic-spec.md).
