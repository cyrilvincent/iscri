import datetime
from sqlalchemy import select
from abc import ABCMeta, abstractmethod
import csv
import time
from sqlentities import Event, File

time0 = time.perf_counter()


class BaseParser(metaclass=ABCMeta):
    """
    Classe de base des parser
    """


    def __init__(self, context):
        """
        path = file path
        file = SqlAlchemy file
        row_num = parser current row
        nb_row = total nb row of the file
        nb_ram = nb row in self.events
        events = list of event
        :param context: SqlAlchemy context
        """
        self.path: str | None = None
        self.file: File | None = None
        self.context = context
        self.row_num = 0
        self.nb_row = 0
        self.nb_ram = 0
        self.events: set[int] = set()
        # self.actors: dict[tuple[str, str, str, str, str, str, str, str, str, str], Actor] = {}
        # self.geos: dict[tuple[int, str], Geo] = {}

    def get_file(self) -> File:
        """
        Querying the file in db path the path
        If the file does not exist, the file is created in db
        :return: the file
        """
        name = self.path
        if "/" in name:
            name = self.path.split("/")[-1]
        name += ".zip"
        y = int(name[:4])
        if y <= 2005:
            name = f"{y}.zip"
            date = datetime.date(y, 12, 31)
        else:
            ym = int(name[:6])
            if ym <= 201303:
                name = f"{ym}.zip"
                date = datetime.date(y, int(name[4:6]), 1)
            else:
                date = datetime.date(y, int(name[4:6]), int(name[6:8]))
        self.file = self.context.session.execute(
            select(File).where(File.name == name)).scalars().first()
        if self.file is None:
            print(f"Warning: file {name} does not exist in db, creating it")
            # quit(3)
            self.file = File()
            self.file.id = int(name.split(".")[0])
            self.file.name = name
            self.file.online_date = self.file.download_date = self.file.dezip_date = datetime.datetime.now()
            self.file.date = date  # Never tested
            self.context.session.add(self.file)
        self.file.import_start_date = datetime.datetime.now()

    def load_cache(self):
        """
        Load all events.global_event_id of a file in a dict
        :return:
        """
        print("Making cache")
        l = self.context.session.execute(select(Event.global_event_id).where(Event.file_id == self.file.id)).scalars().all()
        for e in l:
            self.events.add(e)
            self.nb_ram += 1
        # l = self.context.session.execute(select(Actor)).scalars().all()
        # for e in l:
        #     self.actors[e.key] = e
        #     self.nb_ram += 1
        # l = self.context.session.execute(select(Geo)).scalars().all()
        # for e in l:
        #     self.geos[e.key] = e
        #     self.nb_ram += 1
        print(f"{self.nb_ram} objects in cache")

    def test_file(self, path, encoding="utf8"):
        """
        Test if the file is valid
        :param path: file path
        :param encoding: encoding
        """
        with open(path, encoding=encoding) as f:
            for _ in f:
                self.nb_row += 1
        print(f"Found {self.nb_row} rows")

    def get_int(self, v):
        """
        Return int value of v
        :param v: the value
        :return: int value
        """
        return None if v == "" else int(v)

    def get_float(self, v):
        """
        Return float value of v
        :param v: the value
        :return: float value
        """
        return None if v == "" else float(v)

    def try_float(self, v):
        """
        Return the float value of v, if v is not a float return None
        :param v: the value
        :return: the float value of None
        """
        try:
            return self.get_float(v)
        except:
            return None

    def get_str(self, v):
        """
        if v is None return None
        :param v: the value
        :return: v
        """
        return None if v == "" else v

    def get_date(self, s: str) -> datetime.date:
        """
        Transform s in datetile
        :param s: the value
        :return: the date
        """
        return None if s == "" else datetime.datetime.strptime(s, "%Y%m%d").date()

    def get_bool(self, v):
        """
        Return the bool value of v
        :param v: the value
        :return: the bool
        """
        return None if v == '' else v == "1" or v == "Y"

    def strip_quotes(self, s: str) -> str:
        """
        Not used
        """
        if len(s) > 0 and ((s[0] == '"' and s[-1] == '"') or (s[0] == "'" and s[-1] == "'")):
            return s[1:-1]
        return s

    def duration(self, coef=0.0):
        """
        Display duration
        :param coef: not used
        """
        duration = time.perf_counter() - time0 + 1e-6
        print(f"Parse {self.row_num} rows {(self.row_num / self.nb_row) * 100:.1f}% "
              f"in {duration:.0f}s "
              f"@{self.row_num / duration:.0f}row/s "
              f"{((self.nb_row / self.row_num) * duration) - duration + (duration / (self.row_num / self.nb_row)) * coef:.0f}s remaining ")

    def load(self, path: str, delimiter='\t', encoding="utf8", header=False):
        """
        Parse a file
        :param path: the file path
        :param delimiter: delimiter
        :param encoding: encoding
        :param header: has header
        """
        print(f"Loading {path}")
        self.path = path
        self.test_file(path, encoding)
        self.get_file()
        self.load_cache()
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
                    self.duration(2.0)

        self.post_load()
        self.file.import_end_date = datetime.datetime.now()
        print("Committing")
        self.context.session.commit()
        self.duration(0.01)

    @abstractmethod
    def parse_row(self, row): ...

    @abstractmethod
    def mapper(self, row): ...

    def post_load(self):
        pass

