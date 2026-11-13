PySpark + Azure Databricks Training Datasets
================================================
Core scenario: ShopStream / retail e-commerce.
Files:
- customers.csv: customer master with intentional quality issues.
- products.csv: product dimension.
- orders.csv, order_items.csv, payments.csv: June 2026 transactional facts.
- dirty_daily_sales.csv: intentionally dirty file for cleaning exercises.
- customer_updates_cdc.csv: CDC-style customer changes.
- clickstream_nested.json: nested JSON with structs, arrays, and maps.
- streaming_orders/: five micro-batch JSON files for Structured Streaming / Auto Loader labs.
- skew_sales.csv: intentionally skewed city distribution for performance tuning.
- city_region_lookup.csv: small lookup suitable for broadcast join examples.
- inventory_snapshot.csv: snapshot dataset for partitioning / incremental exercises.

All data is synthetic and intended for classroom use.
