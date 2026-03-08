"""

Sets up the SQLite catalog and creates the medallion namespaces. 

Run this once when setting up the project for the first time. 
    python ingest/init_catalog.py
"""
from pyiceberg.catalog.sql import SqlCatalog

from config.catalog import CATALOG_PATH, WAREHOUSE_PATH, get_catalog

# Config 
NAMESPACES = ["bronze", "silver", "gold"]

def main():
    print("Initialising frostlake catalog...")
    CATALOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    WAREHOUSE_PATH.mkdir(parents=True, exist_ok=True)
    
    catalog = get_catalog()

    for ns in NAMESPACES:
        if not catalog.namespace_exists(ns):
            catalog.create_namespace(ns)
            print(f" Created namespaces: {ns}")
        else:
            print(f" Namespace already exists: {ns}")
    
    print("Catalog init successfully.")


if __name__ == "__main__":
    main()
