from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("registration_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("age", IntegerType(), True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("subcategory", StringType(), True),
    StructField("list_price", DoubleType(), True),
    StructField("cost_price", DoubleType(), True),
    StructField("status", StringType(), True),
])

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_ts", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("shipping_city", StringType(), True),
    StructField("channel", StringType(), True),
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_item_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount_pct", DoubleType(), True),
    StructField("line_amount", DoubleType(), True),
])

PAYMENTS_SCHEMA = StructType([
    StructField("payment_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("payment_mode", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("payment_status", StringType(), True),
    StructField("payment_ts", StringType(), True),
])
