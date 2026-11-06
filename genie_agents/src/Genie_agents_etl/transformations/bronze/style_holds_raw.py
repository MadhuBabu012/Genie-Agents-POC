"""Bronze raw feed: style_holds."""
from pyspark import pipelines as dp

from common import bronze_name, feed_grain, ingest

FEED = "style_holds"


@dp.table(
    name=bronze_name(FEED),
    comment=f"Bronze raw feed {FEED} - all columns as text, unmodified. Grain: {feed_grain(FEED)}.",
)
def style_holds_raw():
    return ingest(FEED)