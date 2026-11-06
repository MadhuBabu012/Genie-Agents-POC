"""Bronze raw feed: warehouse_stock_daily."""
from pyspark import pipelines as dp

from common import bronze_name, feed_grain, ingest

FEED = "warehouse_stock_daily"


@dp.table(
    name=bronze_name(FEED),
    comment=f"Bronze raw feed {FEED} - all columns as text, unmodified. Grain: {feed_grain(FEED)}.",
)
def warehouse_stock_daily_raw():
    return ingest(FEED)