import datetime
from typing import Dict, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.orm import joinedload
from abc import ABCMeta, abstractmethod
import csv
import time
import re

from sqlentities import Event, File, Actor, Geo

time0 = time.perf_counter()


class BaseParser(metaclass=ABCMeta):

    def __init__(self, context):
        self.path: str | None = None
        self.file: File | None = None
        self.context = context
        self.row_num = 0
        self.nb_row = 0
        self.nb_ram = 0
        self.is_new_file = False
        self.events: dict[int, Event] = {}
        self.actors: dict[tuple[str, str, str, str, str, str, str, str, str, str], Actor] = {}
        self.geos: dict[tuple[int, str], Geo] = {}

    def get_file(self):
        name = self.path
        if "/" in name:
            name = self.path.split("/")[-1]
        self.file = self.context.session.execute(
            select(File).where(File.name == name)).scalars().first()
        if self.file is None:
            self.file = File()
            self.file.name = name
            self.file.import_start_date = datetime.datetime.now()
            self.file.date = int(name.split(".")[0])
            self.is_new_file = True
            self.context.session.add(self.file)
            self.context.session.commit()

    def load_cache(self):
        print("Making cache")
        l = self.context.session.execute(select(Event).where(Event.file_id == self.file.id)).scalars().all()
        for e in l:
            self.events[e.id] = e
            self.nb_ram += 1
        l = self.context.session.execute(select(Actor)).scalars().all()
        for e in l:
            self.actors[e.key] = e
            self.nb_ram += 1
        l = self.context.session.execute(select(Geo)).scalars().all()
        for e in l:
            self.geos[e.key] = e
            self.nb_ram += 1
        print(f"{self.nb_ram} objects in cache")

    def test_file(self, path, encoding="utf8"):
        with open(path, encoding=encoding) as f:
            for _ in f:
                self.nb_row += 1
        print(f"Found {self.nb_row} rows")

    def get_int(self, v):
        return None if v == "" else int(v)

    def get_float(self, v):
        return None if v == "" else float(v)

    def get_str(self, v):
        return None if v == "" else v

    def get_date(self, s: str) -> datetime.date:
        return None if s == "" else datetime.datetime.strptime(s, "%Y%m%d").date()

    def strip_quotes(self, s: str) -> str:
        if len(s) > 0 and ((s[0] == '"' and s[-1] == '"') or (s[0] == "'" and s[-1] == "'")):
            return s[1:-1]
        return s

    def load(self, path: str, delimiter='\t', encoding="utf8", header=False):
        print(f"Loading {path}")
        self.path = path
        self.test_file(path, encoding)
        self.get_file()
        self.load_cache()
        duration_cache = time.perf_counter() - time0
        with open(path, encoding=encoding) as f:
            if header:
                reader = csv.DictReader(f, delimiter=delimiter, quotechar="|")
            else:
                reader = csv.reader(f, delimiter=delimiter)
            for row in reader:
                self.row_num += 1
                self.parse_row(row)
                if self.row_num % 10000 == 0 or self.row_num == 10 or self.row_num == 100 \
                        or self.row_num == 1000 or self.row_num == self.nb_row:
                    duration = time.perf_counter() - time0 - duration_cache + 1e-6
                    print(f"Parse {self.row_num} rows {(self.row_num / self.nb_row) * 100:.1f}% "
                          f"in {(duration + duration_cache):.0f}s "
                          f"@{self.row_num / duration:.0f}row/s "
                          f"{((self.nb_row / self.row_num) * duration) - duration + 1:.0f}s remaining ")
        self.post_load()
        self.file.import_end_date = datetime.datetime.now()
        self.context.session.commit()


    @abstractmethod
    def parse_row(self, row): ...

    @abstractmethod
    def mapper(self, row): ...

    def post_load(self):
        pass

