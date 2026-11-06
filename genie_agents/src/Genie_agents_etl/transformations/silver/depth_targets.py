"""Silver: depth_targets - one row per store grade x size class."""
from pyspark import pipelines as dp

from common import silver_name, silver_transform


@dp.table(
    name=silver_name("depth_targets"),
    comment="Silver depth targets - one row per store grade x size class.",
)
@dp.expect_all({"target keys present": "grade IS NOT NULL AND size_class IS NOT NULL"})
def depth_targets():
    return silver_transform("depth_targets")