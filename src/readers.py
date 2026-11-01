def read_csv(spark, path, schema):
    return (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(path)
    )
