# Simple Source → Bronze → Silver → Gold Demo

This is a deliberately small teaching pipeline using only the official `orders.csv`.

## Run order

1. `00_SOURCE_SETUP.ipynb`
2. `01_BRONZE.ipynb`
3. `02_SILVER.ipynb`
4. `03_GOLD.ipynb`

## Flow

```text
orders.csv
126 rows
   ↓
Bronze
126 rows
   ↓
Silver
126 rows
   ↓
Gold
4 rows
```

## Expected Gold result

```text
DELIVERED  78
SHIPPED    29
PLACED     10
CANCELLED   9
```

## Objects

```text
Bronze table:
demo_orders_bronze

Silver table:
demo_orders_silver

Gold table:
demo_order_status_gold

Source Volume:
demo_source_files
```


```text
Source
= incoming file

Bronze
= preserve source + ingestion metadata

Silver
= clean, standardize, type, validate

Gold
= aggregate into business-facing output
```
