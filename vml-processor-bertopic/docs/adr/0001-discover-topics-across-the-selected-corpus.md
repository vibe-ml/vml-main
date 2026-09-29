# Discover topics across the selected corpus

The processor will fit discovery topics across all works satisfying the configured filters and publication interval, with all input languages included. We select one shared discovery boundary instead of separate fits by OpenAlex field or curated technology scope, preserving the requested corpus coverage and allowing cross-field and cross-language themes. This choice requires validating language-driven separation and dominant-field effects; changing to separate fits would change topic membership and invalidate direct comparisons of topic identities. Generated summaries and labels will be in English.

This is an accepted design decision, not implemented behavior. See the [processor spec](../agents/tasks/bertopic/bertopic-spec.md).
