# 🏗️ Retail Mini Data Lake (Python + SCD Type-2)

A lightweight retail data lake built with Python.  
It ingests raw CSV snapshots, performs data cleaning into a processed zone, and maintains a curated **SCD Type-2** product dimension using SQLite.  
The project demonstrates historical tracking of product price changes — a core data engineering skill.

---

## 🚀 Features
- **Raw Zone:** Timestamped CSV ingestion simulating batch arrivals  
- **Processed Zone:** Cleaned and standardized Parquet files  
- **Curated Zone:** SQLite-backed SCD Type-2 dimension table  
- Historical versioning of product attributes (e.g., price changes)  
- Modular scripts for each pipeline stage  

---

## 📂 Project Structure
```

retail-datalake/
├─ data/
│  ├─ raw/          # Landed raw CSVs
│  ├─ processed/    # Cleaned Parquet files
│  └─ curated/      # SQLite DB with SCD Type-2
├─ sample_data/
├─ scripts/
│  ├─ ingest_raw.py
│  ├─ process.py
│  └─ scd_type2.py
└─ requirements.txt

````

---

## ▶️ How to Run the Pipeline

### 1. Ingest a snapshot
```bash
python scripts/ingest_raw.py products_snapshot_1.csv
````

### 2. Process raw files → cleaned Parquet

```bash
python scripts/process.py
```

### 3. Apply SCD Type-2 merges

```bash
python scripts/scd_type2.py
```

---

## 📝 Example SCD Output

Each product keeps a full version history:

| product_id | price | start_date | end_date   | is_current |
| ---------- | ----- | ---------- | ---------- | ---------- |
| P002       | 5.49  | 2025-12-01 | 2025-12-01 | 0          |
| P002       | 6.49  | 2025-12-02 | NULL       | 1          |

---

## 📦 Requirements

Install with:

```bash
pip install -r requirements.txt
```

---

