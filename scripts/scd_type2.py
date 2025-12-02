from pathlib import Path
import pandas as pd
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine, text

PROCESSED = Path("data/processed")
CURATED = Path("data/curated")
CURATED.mkdir(parents=True, exist_ok=True)

DB_PATH = CURATED / "product_dim.db"
ENGINE = create_engine(f"sqlite:///{DB_PATH}")

DIM_TABLE = "product_dim"

def ensure_table():
    """Creates product_dim table if not exists"""
    with ENGINE.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {DIM_TABLE} (
                product_key INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id TEXT NOT NULL,
                product_name TEXT,
                category TEXT,
                price REAL,
                currency TEXT,
                start_date TEXT,
                end_date TEXT,
                is_current INTEGER,
                created_at TEXT
            );
        """))

def fetch_current_dim(conn):
    """Return current active rows"""
    return pd.read_sql(f"SELECT * FROM {DIM_TABLE} WHERE is_current=1", conn)

def apply_scd_for_df(df: pd.DataFrame):
    """Apply SCD Type-2 for each row in the dataframe"""
    today = date.today()
    today_str = today.isoformat()
    yesterday_str = (today - timedelta(days=1)).isoformat()

    with ENGINE.begin() as conn:
        ensure_table()
        current = fetch_current_dim(conn)

        for _, row in df.iterrows():
            bid = str(row["product_id"])
            current_rec = current[current["product_id"] == bid]

            # 1️⃣ New product → insert as current
            if current_rec.empty:
                conn.execute(text(f"""
                    INSERT INTO {DIM_TABLE}
                    (product_id, product_name, category, price, currency,
                     start_date, end_date, is_current, created_at)
                    VALUES (:product_id, :product_name, :category, :price,
                            :currency, :start_date, NULL, 1, :created_at)
                """), {
                    "product_id": bid,
                    "product_name": row["product_name"],
                    "category": row["category"],
                    "price": float(row["price"]),
                    "currency": row["currency"],
                    "start_date": today_str,
                    "created_at": datetime.utcnow().isoformat()
                })
                current = fetch_current_dim(conn)
                continue

            # 2️⃣ Existing record — check if anything changed
            current_row = current_rec.iloc[0]
            changed = False
            for attr in ["product_name", "category", "price", "currency"]:
                if str(row[attr]) != str(current_row[attr]):
                    changed = True
                    break

            # No change → skip
            if not changed:
                continue

            # 3️⃣ Expire old + insert new version
            conn.execute(text(f"""
                UPDATE {DIM_TABLE}
                SET end_date = :end_date, is_current = 0
                WHERE product_key = :pk
            """), {
                "end_date": yesterday_str,
                "pk": int(current_row["product_key"])
            })

            conn.execute(text(f"""
                INSERT INTO {DIM_TABLE}
                (product_id, product_name, category, price, currency,
                 start_date, end_date, is_current, created_at)
                VALUES (:product_id, :product_name, :category, :price,
                        :currency, :start_date, NULL, 1, :created_at)
            """), {
                "product_id": bid,
                "product_name": row["product_name"],
                "category": row["category"],
                "price": float(row["price"]),
                "currency": row["currency"],
                "start_date": today_str,
                "created_at": datetime.utcnow().isoformat()
            })

def run_scd_pipeline():
    parquet_files = sorted(PROCESSED.glob("*.parquet"))
    if not parquet_files:
        print("No processed files found. Run process.py first.")
        return

    for pq in parquet_files:
        df = pd.read_parquet(pq)
        apply_scd_for_df(df)

    print(f"SCD Type-2 merge completed → {DB_PATH}")

if __name__ == "__main__":
    run_scd_pipeline()
