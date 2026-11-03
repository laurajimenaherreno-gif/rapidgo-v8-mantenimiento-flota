import os
import sys

# Permite importar calidad_flota.py y dag_flota.py desde airflow/dags
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "airflow", "dags"))