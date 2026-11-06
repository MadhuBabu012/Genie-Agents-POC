"""Bronze raw feed: stores."""
from pyspark import pipelines as dp

from common import bronze_name, feed_grain, ingest

FEED = "stores"


@dp.table(
    name=bronze_name(FEED),
    comment=f"Bronze raw feed {FEED} - all columns as text, unmodified. Grain: {feed_grain(FEED)}.",
)
def stores_raw():
    return ingest(FEED)