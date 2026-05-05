#!/bin/bash
# Skrip untuk membantu setup koneksi Spark di Airflow WSL
source ~/airflow_venv/bin/activate
export AIRFLOW_HOME=~/airflow

# Hapus jika sudah ada (opsional) untuk update
airflow connections delete 'spark_default' 2>/dev/null

# Tambah koneksi baru
airflow connections add 'spark_default' \
    --conn-type 'spark' \
    --conn-host 'local[*]' \
    --conn-extra '{"queue": "default"}'

echo "Koneksi 'spark_default' berhasil dikonfigurasi."
