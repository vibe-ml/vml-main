"""SQLAlchemy models for OpenAlex raw data and technical metadata."""

from src.models.base import Base
from src.models.raw import (
    TaxonomyBundles,
    TaxonomyObservations,
    WorkCurrent,
    WorkObservations,
    WorkProcessing,
    WorkVersions,
)
from src.models.tmd import (
    TaxonomyRawFiles,
    TaxonomyRuns,
    WorkApiAllowances,
    WorkApiPages,
    WorkBaselines,
    WorkBatchClaims,
    WorkBatches,
    WorkChunks,
    WorkPartitions,
    WorkReleaseFiles,
    WorkReleases,
    WorkRuns,
    WorkScans,
    WorkScopes,
    WorkSources,
)

__all__ = [
    "Base",
    "TaxonomyBundles",
    "TaxonomyObservations",
    "TaxonomyRawFiles",
    "TaxonomyRuns",
    "WorkApiAllowances",
    "WorkApiPages",
    "WorkBaselines",
    "WorkBatchClaims",
    "WorkBatches",
    "WorkChunks",
    "WorkCurrent",
    "WorkObservations",
    "WorkPartitions",
    "WorkProcessing",
    "WorkReleaseFiles",
    "WorkReleases",
    "WorkRuns",
    "WorkScans",
    "WorkScopes",
    "WorkSources",
    "WorkVersions",
]
