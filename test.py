print("ISCRI prerequesites")
print("===================")
print()
import sys
print(f"Python version: {sys.version}")
import config
print(f"ISCRI version: {config.version}")
import psycopg2
print(f"Psycopg2 version: {psycopg2.__version__}")
import sqlalchemy
print(f"SqlAlchemy version: {sqlalchemy.__version__}")
import jupyterlab
print(f"JupyterLab version: {jupyterlab.__version__}")
import numpy
print(f"Numpy version: {numpy.__version__}")
import scipy
print(f"Scipy version: {scipy.__version__}")
import pandas
print(f"Pandas version: {pandas.__version__}")
import plotly
print(f"Plotly version: {plotly.__version__}")
import ipywidgets
print(f"IPywidgets version: {ipywidgets.__version__}")
print(f"Connecting to: {config.connection_string}")
from dbcontext import Context
context = Context()
context.create()
db_size = context.db_size()
print(f"Database {context.db_name}: {db_size:.0f} MB")
import scrapper
s = scrapper.GdeltScrapper(context, fake_download=True, no_commit=True)
file = s.get_last_file()
print(f"Last file: {file.name}")
s.test()
