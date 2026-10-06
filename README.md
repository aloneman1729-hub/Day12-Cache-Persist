# Day 12 — Cache and Persist

## Overview

This project demonstrates Apache Spark RDD caching and persistence.

A cleaned transaction dataset is reused to generate three reports:

1. Revenue Report
2. Product Sales Report
3. Customer Sales Report

The cleaned RDD is persisted using `MEMORY_AND_DISK` so that repeated actions can reuse the computed data.

## Scenario

The input contains 15 transactions:

- 13 COMPLETED transactions
- 2 CANCELLED transactions

The cleaning process removes CANCELLED transactions.

Therefore, the cleaned RDD contains 13 transactions.

## Project Structure

```text
Day12-Cache-Persist/
├── data/
│   └── transactions.csv
├── src/
│   ├── cache_persist_demo.py
│   ├── cache_vs_persist.py
│   └── storage_level_experiment.py
├── tests/
│   └── test_cache_persist.py
├── .gitignore
├── requirements.txt
└── README.md
Cache

cache() stores an RDD using Spark's default storage level.

rdd.cache()
Persist

persist() allows an explicit storage level.

rdd.persist(StorageLevel.MEMORY_AND_DISK)
Storage Levels
MEMORY_ONLY

Stores partitions in memory.

MEMORY_AND_DISK

Stores partitions in memory and spills partitions to disk when necessary.

DISK_ONLY

Stores partitions only on disk.

Cache vs Persist
Feature	cache()	persist()
Purpose	Reuse RDD	Reuse RDD
Storage level	Default	Explicit
Flexibility	Low	High
Three Reports
Revenue
Total revenue: 163000.00
Product Sales
Headphones: 6
Keyboard: 4
Laptop: 2
Monitor: 2
Mouse: 7
Customer Sales
C101: 68000.00
C102: 5000.00
C103: 17000.00
C104: 8000.00
C106: 13000.00
C107: 52000.00
Lazy Evaluation

Spark transformations are lazy. Computation happens when an action such as count(), sum(), or collect() is executed.

The first action materializes the persisted RDD. Later actions can reuse the persisted partitions.

When Caching Helps

Caching is useful when:

The same RDD is reused multiple times.
Transformations are expensive.
Multiple reports depend on the same dataset.
When Caching Hurts

Caching can hurt when:

The RDD is used only once.
Recomputing the RDD is cheap.
The dataset is too large for available memory.
Memory pressure causes eviction or garbage collection.
Storage overhead is greater than the recomputation cost.
Tests

Run:

python -m pytest -v

Expected:

3 passed
Run the Project
spark-submit src/cache_persist_demo.py
spark-submit src/storage_level_experiment.py
spark-submit src/cache_vs_persist.py
Technologies
Python 3.12.3
PySpark 3.5.6
Apache Spark 3.5.6
pytest 9.1.1
WSL / Ubuntu
