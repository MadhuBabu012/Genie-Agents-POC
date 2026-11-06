"""Silver: clearance_styles - one row per end-of-season style."""
from pyspark import pipelines as dp

from common import silver_name, silver_transform


@dp.table(
    name=silver_name("clearance_styles"),
    comment="Silver clearance styles - one row per end-of-season style.",
)
@dp.expect_all({"style present": "style_code IS NOT NULL"})
def clearance_styles():
    return silver_transform("clearance_styles")