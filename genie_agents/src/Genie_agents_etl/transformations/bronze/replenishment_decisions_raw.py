"""Bronze raw feed: replenishment_decisions."""
from pyspark import pipelines as dp

from common import bronze_name, feed_grain, ingest

FEED = "replenishment_decisions"


@dp.table(
    name=bronze_name(FEED),
    comment=f"Bronze raw feed {FEED} - all columns as text, unmodified. Grain: {feed_grain(FEED)}.",
)
def replenishment_decisions_raw():
    return ingest(FEED)