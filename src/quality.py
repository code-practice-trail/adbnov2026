from pyspark.sql import functions as F

def duplicate_keys(input_df, key_column):
    return (
        input_df
        .groupBy(key_column)
        .count()
        .filter(F.col("count") > 1)
    )

def orphan_keys(child_df, parent_df, key_column):
    return (
        child_df.select(key_column).distinct()
        .join(
            parent_df.select(key_column).distinct(),
            key_column,
            "left_anti",
        )
    )

def require_zero_rows(invalid_df, rule_name):
    if invalid_df.limit(1).count() > 0:
        raise AssertionError(f"DQ check failed: {rule_name}")
