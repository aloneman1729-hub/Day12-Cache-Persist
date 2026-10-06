from pyspark import SparkContext, SparkConf, StorageLevel


def create_spark_context():
    conf = (
        SparkConf()
        .setAppName("CacheVsPersist")
        .setMaster("local[*]")
        .set("spark.hadoop.fs.defaultFS", "file:///")
    )

    sc = SparkContext.getOrCreate(conf=conf)
    sc.setLogLevel("WARN")

    return sc


def main():
    sc = create_spark_context()

    transactions = (
        sc.textFile("data/transactions.csv")
        .filter(lambda line: not line.startswith("transaction_id"))
        .filter(lambda line: ",COMPLETED" in line)
    )

    cached_rdd = transactions.map(lambda line: line.split(","))
    cached_rdd.cache()

    print("\n===== CACHE() =====")
    print("Storage level before action:")
    print(cached_rdd.getStorageLevel())

    print("Count:", cached_rdd.count())

    print("Storage level after action:")
    print(cached_rdd.getStorageLevel())

    cached_rdd.unpersist()

    persisted_rdd = transactions.map(lambda line: line.split(","))
    persisted_rdd.persist(StorageLevel.MEMORY_AND_DISK)

    print("\n===== PERSIST(MEMORY_AND_DISK) =====")
    print("Storage level before action:")
    print(persisted_rdd.getStorageLevel())

    print("Count:", persisted_rdd.count())

    print("Storage level after action:")
    print(persisted_rdd.getStorageLevel())

    persisted_rdd.unpersist()

    print("\n===== CACHE VS PERSIST =====")
    print("cache()   -> uses the default RDD storage level")
    print("persist() -> allows an explicit storage level")
    print("Example   -> MEMORY_AND_DISK")

    sc.stop()


if __name__ == "__main__":
    main()
