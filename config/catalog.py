from pathlib import Path
from pyiceberg.catalog.sql import SqlCatalog


CATALOG_PATH = Path("catalog/frostlake.db")
WAREHOUSE_PATH = Path("warehouse")


def get_catalog() -> SqlCatalog:
    return SqlCatalog(
        "frostlake", 
        **{
            "uri": f"sqlite:///{CATALOG_PATH}",
            "warehouse": str(WAREHOUSE_PATH),
        }
    )