"""Silver: style_holds - one row per blocked style."""
from pyspark import pipelines as dp

from common import silver_name, silver_transform


@dp.table(
    name=silver_name("style_holds"),
    comment="Silver style holds - one row per style blocked from replenishment.",
)
@dp.expect_all({"style present": "style_code IS NOT NULL"})
def style_holds():
    return silver_transform("style_holds")