import shutil
from pathlib import Path
from datetime import datetime
import sys

RAW = Path("data/raw")
SAMPLE = Path("sample_data")
RAW.mkdir(parents=True, exist_ok=True)

def ingest(filename):
    src = SAMPLE / filename
    if not src.exists():
        raise FileNotFoundError(f"Sample file missing: {src}")

    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    dest = RAW / f"{src.stem}_{ts}{src.suffix}"
    shutil.copy(src, dest)
    print(f"Ingested → {dest}")

if __name__ == "__main__":
    # Require explicit filename from command-line arguments
    if len(sys.argv) < 2:
        raise ValueError("Usage: python ingest_raw.py <filename>")
    
    ingest(sys.argv[1])
