import datetime

import art
import pandas as pd
from sqlalchemy import select, text

import config
import argparse

from dbcontext import Context

art.tprint(config.name, "big")
print("Country Parser")
print("==============")
print(f"V{config.version}")
print(config.copyright)
print()
context = Context()
context.create()
path = "data/countries.csv"
print(f"Load {path}")
df = pd.read_csv(path)
df.index.names = ["id"]
context.create_engine()
with context.engine.begin() as connection:
    df.to_sql("country", connection, if_exists="replace", index=True)
    connection.execute(text("ALTER TABLE country ADD PRIMARY KEY (id)"))



