"""
Verify bronze layer tables.

"""

from config.catalog import get_catalog

def verify_table(catalog, namespace: str, table_name: str):
    table      = catalog.load_table((namespace, table_name))
    scan       = table.scan()
    arrow      = scan.to_arrow()
    row_count  = len(arrow)
    print(f"\nbronze.{table_name} — {row_count} rows")
    print(arrow.slice(0, 3).to_pandas().to_string(index=False))

def main():
    catalog = get_catalog()
    for table in ["customers", "mortgages", "loans"]:
        verify_table(catalog, "bronze", table)

if __name__ == "__main__":
    main()