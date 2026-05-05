"""Seed KelasJuara CSV source data into MongoDB collections.

This script imports the generated source CSV files from
`sumber_data_new/kelasjuara` into the MongoDB database `db_kelasjuara`.
"""
from __future__ import annotations

import glob
import os
from pathlib import Path

import pandas as pd
from pymongo import MongoClient


BASE_DIR = Path(r"d:\SEMESTER 6\TPD\UTS")
CSV_DIR = BASE_DIR / "sumber_data_new" / "kelasjuara"
MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "db_kelasjuara"

# CSV filename -> Mongo collection name
COLLECTIONS = {
    "pengguna": "pengguna",
    "produk_kelas": "produk_kelas",
    "pembelian_kelas": "pembelian_kelas",
    "akses_materi": "akses_materi",
    "skor_evaluasi": "skor_evaluasi",
    "invoice_pembayaran": "invoice_pembayaran",
    "review_kelas": "review_kelas",
}


def import_csv_to_mongo() -> None:
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    if not CSV_DIR.exists():
        raise FileNotFoundError(f"Source directory not found: {CSV_DIR}")

    imported = 0
    for csv_name, collection_name in COLLECTIONS.items():
        file_path = CSV_DIR / f"{csv_name}.csv"
        if not file_path.exists():
            print(f"Skipping missing file: {file_path}")
            continue

        print(f"Importing {file_path.name} -> {MONGO_DB}.{collection_name} ...")
        df = pd.read_csv(file_path)
        records = df.to_dict(orient="records")

        db[collection_name].drop()
        if records:
            db[collection_name].insert_many(records)
            print(f"  Imported {len(records)} documents.")
        else:
            print("  No rows found; collection cleared.")
        imported += 1

    print(f"Done. Imported {imported} collection(s) into MongoDB database '{MONGO_DB}'.")


if __name__ == "__main__":
    import_csv_to_mongo()
