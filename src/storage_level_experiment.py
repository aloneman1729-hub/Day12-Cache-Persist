from pyspark import SparkContext, SparkConf, StorageLevel


def create_spark_context():
    conf = (
        SparkConf()
        .setAppName("StorageLevelExperiment")
        .setMaster("local[*]")
        .set("spark.hadoop.fs.defaultFS", "file:///")
    )

    sc = SparkContext.getOrCreate(conf=conf)
    sc.setLogLevel("WARN")

    return sc


def run_experiment(sc, storage_level, level_name):
    print(f"\n===== {level_name} =====")

    rdd = (
        sc.textFile("data/transactions.csv")
        .filter(lambda line: not line.startswith("transaction_id"))
        .filter(lambda line: ",COMPLETED" in line)
    )

    rdd.persist(storage_level)

    first_count = rdd.count()
    second_count = rdd.count()

    print(f"First count  : {first_count}")
    print(f"Second count : {second_count}")
    print(f"Storage level: {rdd.getStorageLevel()}")

    rdd.unpersist()


def main():
    sc = create_spark_context()

    run_experiment(
        sc,
        StorageLevel.MEMORY_ONLY,
        "MEMORY_ONLY"
    )

    run_experiment(
        sc,
        StorageLevel.MEMORY_AND_DISK,
        "MEMORY_AND_DISK"
    )

    run_experiment(
        sc,
        StorageLevel.DISK_ONLY,
        "DISK_ONLY"
    )

    sc.stop()


if __name__ == "__main__":
    main()
