"""Bronze raw feed: pos_sales."""
from pyspark import pipelines as dp

from common import bronze_name, feed_grain, ingest

FEED = "pos_sales"


@dp.table(
    name=bronze_name(FEED),
    comment=f"Bronze raw feed {FEED} - all columns as text, unmodified. Grain: {feed_grain(FEED)}.",
)
def pos_sales_raw():
    return ingest(FEED)