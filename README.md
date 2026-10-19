# Data-Warehouse

Cara jalanin streamlitnya:
1. buat virtual environtment dulu pakai `python -m venv venv`, terus aktifin venv nya `venv\Scripts\Activate.ps1`
2. Kalau berhasil dan udah ada tulisan (venv) di awal baris terminal, langsung install library `python -m pip install --upgrade pip` sama `pip install -r requirements.txt`
3. Jalankan ETL lalu dashboard (kondisi venv tetap aktif), pakai `python etl_new.py --once`, terus `streamlit run dashboard.py`
4. Nanti kalau di minta email pas jalanin streamlit bisa dikosongin.
