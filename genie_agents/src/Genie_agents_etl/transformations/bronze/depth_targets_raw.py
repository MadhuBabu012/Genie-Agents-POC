"""Bronze raw feed: depth_targets."""
from pyspark import pipelines as dp

from common import bronze_name, feed_grain, ingest

FEED = "depth_targets"


@dp.table(
    name=bronze_name(FEED),
    comment=f"Bronze raw feed {FEED} - all columns as text, unmodified. Grain: {feed_grain(FEED)}.",
)
def depth_targets_raw():
    return ingest(FEED)