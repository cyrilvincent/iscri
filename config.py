import os
name = "ISCRI"
version = "1.0.0.RC"
copyright = "(c) Skema 2024-2025"
connection_string = "postgresql://postgres:sa@localhost:5432/iscri"
download_path = "d:/iscri/download2" if os.name == "nt" else "/home/azureuseriscri/download"
iscri_threshold = 1e-2
nb_row_commit = 1000000


