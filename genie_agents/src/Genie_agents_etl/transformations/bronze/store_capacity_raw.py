"""Bronze raw feed: store_capacity."""
from pyspark import pipelines as dp

from common import bronze_name, feed_grain, ingest

FEED = "store_capacity"


@dp.table(
    name=bronze_name(FEED),
    comment=f"Bronze raw feed {FEED} - all columns as text, unmodified. Grain: {feed_grain(FEED)}.",
)
def store_capacity_raw():
    return ingest(FEED)