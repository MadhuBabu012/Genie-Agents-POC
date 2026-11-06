"""Bronze raw feed: in_transit."""
from pyspark import pipelines as dp

from common import bronze_name, feed_grain, ingest

FEED = "in_transit"


@dp.table(
    name=bronze_name(FEED),
    comment=f"Bronze raw feed {FEED} - all columns as text, unmodified. Grain: {feed_grain(FEED)}.",
)
def in_transit_raw():
    return ingest(FEED)