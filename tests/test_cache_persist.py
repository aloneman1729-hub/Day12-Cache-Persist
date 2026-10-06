from pathlib import Path
import sys

from pyspark import SparkContext, SparkConf, StorageLevel

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cache_persist_demo import load_transactions, clean_transactions


def create_test_context():
    conf = (
        SparkConf()
        .setMaster("local[2]")
        .setAppName("CachePersistTests")
        .set("spark.hadoop.fs.defaultFS", "file:///")
    )

    sc = SparkContext.getOrCreate(conf=conf)
    sc.setLogLevel("ERROR")

    return sc


def test_cleaned_transaction_count():
    sc = create_test_context()

    raw = load_transactions(sc, "data/transactions.csv")
    cleaned = clean_transactions(raw)

    assert cleaned.count() == 13

    sc.stop()


def test_cancelled_transactions_are_removed():
    sc = create_test_context()

    raw = load_transactions(sc, "data/transactions.csv")
    cleaned = clean_transactions(raw)

    statuses = cleaned.map(lambda row: row["status"]).collect()

    assert len(statuses) == 13
    assert all(status == "COMPLETED" for status in statuses)

    sc.stop()


def test_memory_and_disk_persist():
    sc = create_test_context()

    raw = load_transactions(sc, "data/transactions.csv")
    cleaned = clean_transactions(raw)

    cleaned.persist(StorageLevel.MEMORY_AND_DISK)

    assert cleaned.count() == 13
    assert cleaned.getStorageLevel() == StorageLevel.MEMORY_AND_DISK

    cleaned.unpersist()

    sc.stop()
