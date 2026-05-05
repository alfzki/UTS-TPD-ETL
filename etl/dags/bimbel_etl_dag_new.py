"""
bimbel_etl_dag.py - Orchestration for Bimbel ETL Pipeline (WSL)
=========================================================================
Pipeline ini mengintegrasikan data dari 4 platform:
1. ZeniBelajar (MySQL)
2. RuangCerdas (PostgreSQL)
3. KelasJuara (CSV)
4. PintarNusa (CSV)

Alur: Extract (paralel) → Transform Staging → Load Staging →
      Transform Warehouse → Load Warehouse
"""
from airflow import DAG
from airflow.utils.task_group import TaskGroup
import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# ──────────────────────────────────────────────
# KONFIGURASI
# ──────────────────────────────────────────────

# Host IP Windows dari WSL (Menggunakan localhost berkat WSL Mirrored Networking)
HOST_IP = "127.0.0.1" 

# Path ke script ETL di drive Windows
ETL_SCRIPT_PATH = "/mnt/d/SEMESTER 6/TPD/UTS/etl/main_etl.py"

# Spark connection ID di Airflow
SPARK_CONN_ID = "spark_default"

# Packages Maven yang diperlukan
SPARK_PACKAGES = ",".join([
    "org.mongodb.spark:mongo-spark-connector_2.13:11.0.1",
    "org.postgresql:postgresql:42.7.3",
    "com.mysql:mysql-connector-j:8.3.0",
])

# Default arguments untuk semua task
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
}

def _spark_task(task_id, step, doc):
    """Helper untuk membuat SparkSubmitOperator task."""
    return SparkSubmitOperator(
        task_id=task_id,
        application=ETL_SCRIPT_PATH,
        application_args=["--step", step],
        conn_id=SPARK_CONN_ID,
        # 'master' dihapus karena sudah diatur di Airflow Connection 'spark_default'
        packages=SPARK_PACKAGES,
        driver_memory="4g",
        executor_memory="2g",
        conf={},
        verbose=True,
        doc_md=doc,
    )


# ──────────────────────────────────────────────
# DAG DEFINITION
# ──────────────────────────────────────────────

with DAG(
    dag_id="bimbel_etl_pipeline",
    default_args=default_args,
    description="ETL Pipeline: 4 sumber bimbel -> staging -> warehouse (incremental append)",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["etl", "bimbel", "data-warehouse"],
    doc_md="""
### Bimbel ETL Pipeline (Incremental Append)
Pipeline ini mengotomatiskan penarikan data dari 4 platform bimbel berbeda
dan mentransformasikannya ke dalam format Data Warehouse (Star Schema).

**Strategi Load:**
- Dimensi: overwrite dengan hash-based key yang stabil
- Fakta: append + deduplikasi (data baru saja yang ditambahkan)

**Alur Task:**
1. **Extraction**: Mengambil data mentah secara paralel dari 4 platform.
2. **Transform Staging**: Normalisasi dan integrasi data ke format staging.
3. **Load Staging**: Menyimpan data staging ke MySQL (append/upsert).
4. **Transform Warehouse**: Membangun tabel dimensi dan fakta.
5. **Load Warehouse**: Menyimpan ke gudang data MySQL (append/upsert).
"""
) as dag:

    with TaskGroup(group_id="extract", tooltip="Task extraction data sumber") as extract_group:
        extract_zenibelajar = _spark_task(
            "extract_zenibelajar", "extract_zenibelajar",
            "Extract data ZeniBelajar dari MongoDB (via Host IP)")

        extract_ruangcerdas = _spark_task(
            "extract_ruangcerdas", "extract_ruangcerdas",
            "Extract data RuangCerdas dari PostgreSQL via JDBC")

        extract_kelasjuara = _spark_task(
            "extract_kelasjuara", "extract_kelasjuara",
            "Extract data KelasJuara dari MySQL via JDBC")

        extract_pintarnusa = _spark_task(
            "extract_pintarnusa", "extract_pintarnusa",
            "Extract data PintarNusa dari Dataset CSV baru")

    with TaskGroup(group_id="staging", tooltip="Transform dan load staging") as staging_group:
        transform_staging = _spark_task(
            "transform_staging", "transform_staging",
            "Transform data mentah ke format staging: standarisasi pengguna, "
            "kelas, konversi satuan, normalisasi skor, mapping lintas platform.")

        load_staging = _spark_task(
            "load_staging", "load_staging",
            "Load staging DataFrames ke MySQL DB. Tabel referensi: overwrite. "
            "Tabel transaksional: append + deduplikasi.")

        transform_staging >> load_staging

    with TaskGroup(group_id="warehouse", tooltip="Transform dan load warehouse") as warehouse_group:
        transform_warehouse = _spark_task(
            "transform_warehouse", "transform_warehouse",
            "Transform staging ke warehouse star schema.")

        load_warehouse = _spark_task(
            "load_warehouse", "load_warehouse",
            "Load warehouse ke MySQL DB. Dimensi: overwrite (key stabil). "
            "Fakta: append + deduplikasi.")

        transform_warehouse >> load_warehouse

    # ── TASK DEPENDENCIES ────────────────────
    extract_group >> staging_group >> warehouse_group
