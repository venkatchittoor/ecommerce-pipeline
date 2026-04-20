import dlt
from pyspark.sql.functions import col, round as spark_round, sum as spark_sum, avg as spark_avg, count as spark_count

# ============================================================
# BRONZE LAYER — Read from existing ecommerce Delta tables
# ============================================================

@dlt.table(
    name="bronze_customers",
    comment="Customer records from existing Delta table"
)
def bronze_customers():
    return spark.table("workspace.ecommerce.bronze_customers")

@dlt.table(
    name="bronze_products",
    comment="Product catalog from existing Delta table"
)
def bronze_products():
    return spark.table("workspace.ecommerce.bronze_products")

@dlt.table(
    name="bronze_orders",
    comment="Order headers from existing Delta table"
)
def bronze_orders():
    return spark.table("workspace.ecommerce.bronze_orders")

@dlt.table(
    name="bronze_order_items",
    comment="Order line items from existing Delta table"
)
def bronze_order_items():
    return spark.table("workspace.ecommerce.bronze_order_items")

# ============================================================
# SILVER LAYER — Clean, join and validate
# ============================================================

@dlt.table(
    name="silver_order_items",
    comment="Cleaned and joined order items"
)
@dlt.expect("valid customer", "customer_id IS NOT NULL")
@dlt.expect("valid order", "order_id IS NOT NULL")
@dlt.expect("positive revenue", "total_price > 0")
@dlt.expect_or_drop("valid status", "status IN ('completed', 'returned', 'pending')")
def silver_order_items():
    orders    = dlt.read("bronze_orders")
    items     = dlt.read("bronze_order_items")
    customers = dlt.read("bronze_customers")
    products  = dlt.read("bronze_products")

    return (
        items
        .join(orders,    "order_id")
        .join(customers, "customer_id")
        .join(products,  "product_id")
        .select(
            items.item_id.cast("long"),
            items.order_id.cast("long"),
            customers.customer_id.cast("long"),
            customers.name.alias("customer_name"),
            customers.country,
            customers.signup_date,
            products.product_id.cast("long"),
            products.name.alias("product_name"),
            products.category,
            orders.order_date,
            orders.status,
            items.quantity.cast("int"),
            items.unit_price.cast("double"),
            items.total_price.cast("double")
        )
        .filter(col("total_price") > 0)
        .filter(col("customer_id").isNotNull())
    )

# ============================================================
# GOLD LAYER — Business aggregations
# ============================================================

@dlt.table(
    name="gold_revenue_by_category",
    comment="Revenue aggregated by product category"
)
def gold_revenue_by_category():
    return (
        dlt.read("silver_order_items")
        .filter(col("status") == "completed")
        .groupBy("category")
        .agg(
            spark_round(spark_sum("total_price"), 2).alias("total_revenue"),
            spark_sum("quantity").cast("long").alias("total_units_sold"),
            spark_round(spark_avg("total_price"), 2).alias("avg_order_value"),
            spark_count("*").alias("order_count")
        )
        .orderBy("total_revenue", ascending=False)
    )

@dlt.table(
    name="gold_top_customers",
    comment="Top customers by lifetime spend"
)
def gold_top_customers():
    return (
        dlt.read("silver_order_items")
        .filter(col("status") != "returned")
        .groupBy("customer_id", "customer_name", "country")
        .agg(
            spark_round(spark_sum("total_price"), 2).alias("total_spend"),
            spark_count("*").alias("order_count"),
            spark_round(spark_avg("total_price"), 2).alias("avg_order_value")
        )
        .orderBy("total_spend", ascending=False)
        .limit(100)
    )

@dlt.table(
    name="gold_return_analysis",
    comment="Return rate and revenue lost by category"
)
def gold_return_analysis():
    all_orders = (
        dlt.read("silver_order_items")
        .groupBy("category")
        .agg(spark_count("*").alias("total_orders"))
    )
    returned = (
        dlt.read("silver_order_items")
        .filter(col("status") == "returned")
        .groupBy("category")
        .agg(
            spark_count("*").alias("total_returns"),
            spark_round(spark_sum("total_price"), 2).alias("total_revenue_lost")
        )
    )
    return (
        all_orders.join(returned, "category", "left")
        .withColumn(
            "return_rate",
            spark_round(col("total_returns") / col("total_orders") * 100, 2)
        )
    )