from pyspark import SparkContext, SparkConf, StorageLevel


def create_spark_context():
    conf = (
        SparkConf()
        .setAppName("CachePersistDemo")
        .setMaster("local[*]")
        .set("spark.hadoop.fs.defaultFS", "file:///")
    )

    sc = SparkContext.getOrCreate(conf=conf)
    sc.setLogLevel("WARN")

    return sc


def load_transactions(sc, file_path):
    return (
        sc.textFile(file_path)
        .filter(lambda line: not line.startswith("transaction_id"))
        .map(lambda line: line.split(","))
    )


def clean_transactions(transactions_rdd):
    return (
        transactions_rdd
        .filter(lambda row: len(row) == 7)
        .filter(lambda row: row[6] == "COMPLETED")
        .map(
            lambda row: {
                "transaction_id": row[0],
                "customer_id": row[1],
                "product": row[2],
                "category": row[3],
                "quantity": int(row[4]),
                "amount": float(row[5]),
                "status": row[6],
            }
        )
    )


def revenue_report(cleaned_rdd):
    return cleaned_rdd.map(lambda row: row["amount"]).sum()


def product_sales_report(cleaned_rdd):
    return (
        cleaned_rdd
        .map(lambda row: (row["product"], row["quantity"]))
        .reduceByKey(lambda a, b: a + b)
        .collect()
    )


def customer_report(cleaned_rdd):
    return (
        cleaned_rdd
        .map(lambda row: (row["customer_id"], row["amount"]))
        .reduceByKey(lambda a, b: a + b)
        .collect()
    )


def main():
    sc = create_spark_context()

    transactions_file = "data/transactions.csv"

    raw_transactions = load_transactions(sc, transactions_file)
    cleaned_transactions = clean_transactions(raw_transactions)

    cleaned_transactions.persist(StorageLevel.MEMORY_AND_DISK)

    print("\n===== DAY 12 — CACHE AND PERSIST =====")

    start = __import__("time").time()
    total_revenue = revenue_report(cleaned_transactions)
    revenue_time = __import__("time").time() - start

    print("\n===== REPORT 1: REVENUE =====")
    print(f"Total revenue : {total_revenue:.2f}")
    print(f"Execution time: {revenue_time:.4f} seconds")

    start = __import__("time").time()
    product_sales = product_sales_report(cleaned_transactions)
    product_time = __import__("time").time() - start

    print("\n===== REPORT 2: PRODUCT SALES =====")

    for product, quantity in sorted(product_sales):
        print(f"{product}: {quantity}")

    print(f"Execution time: {product_time:.4f} seconds")

    start = __import__("time").time()
    customer_sales = customer_report(cleaned_transactions)
    customer_time = __import__("time").time() - start

    print("\n===== REPORT 3: CUSTOMER SALES =====")

    for customer, amount in sorted(customer_sales):
        print(f"{customer}: {amount:.2f}")

    print(f"Execution time: {customer_time:.4f} seconds")

    print("\n===== CACHE / PERSIST INFORMATION =====")
    print("Storage level : MEMORY_AND_DISK")
    print("RDD cached/persisted successfully.")

    cleaned_transactions.unpersist()
    sc.stop()


if __name__ == "__main__":
    main()
