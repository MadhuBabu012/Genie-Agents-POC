"""Bronze raw feed: clearance_styles."""
from pyspark import pipelines as dp

from common import bronze_name, feed_grain, ingest

FEED = "clearance_styles"


@dp.table(
    name=bronze_name(FEED),
    comment=f"Bronze raw feed {FEED} - all columns as text, unmodified. Grain: {feed_grain(FEED)}.",
)
def clearance_styles_raw():
    return ingest(FEED)