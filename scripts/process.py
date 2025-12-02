from pathlib import Path
import pandas as pd
from datetime import datetime
from tqdm import tqdm

RAW = Path("data/raw")
PROCESSED = Path("data/processed")
PROCESSED.mkdir(parents=True, exist_ok=True)

def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    # Standardize columns
    df.columns = [c.strip().lower() for c in df.columns]

    # Trim string columns
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip()

    # Parse updated_at column
    if "updated_at" in df.columns:
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")

    # Convert price to numeric
    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")

    # Add ingestion timestamp
    df["_ingest_ts"] = datetime.utcnow()

    return df


def process_all():
    csv_files = sorted(RAW.glob("*.csv"))
    if not csv_files:
        print("No raw CSVs found. Run ingest_raw.py first.")
        return

    for csv in tqdm(csv_files, desc="Processing CSVs"):
        df = pd.read_csv(csv)
        df = clean_df(df)
        out_path = PROCESSED / f"{csv.stem}.parquet"
        df.to_parquet(out_path, index=False)
        print(f"Processed → {out_path}")

if __name__ == "__main__":
    process_all()
