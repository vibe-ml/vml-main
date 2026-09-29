# Include outliers in the share denominator

ADR 0002 kept outlier works out of TEM share denominators. An emergence observation measures a discovery topic's share of the selected corpus, so each period's denominator counts every selected work in that period, including outliers. Topic `-1` still has no observation and stays out of the median. Excluding outliers from the denominator would raise every topic's share.

This corrects the denominator rule in [ADR 0002](0002-preserve-outliers-exclude-from-tem.md). Emergence observations store monthly share change when two complete months exist, and the annual quadrant when two complete calendar years exist. Succeeded headlines are embedded into `discovery_headlines`. The discovery job scores the run after labels.
