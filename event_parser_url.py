import argparse
import csv
import datetime

from sqlalchemy import select

import config
import art
from base_parser import BaseParser
from dbcontext import Context
from event_parser import EventParser
from sqlentities import Event, Url


class EventUrlParser(EventParser):

    def __init__(self, context, ignore_url=False, nb_row_commit=config.nb_row_commit):
        super().__init__(context, ignore_url, nb_row_commit)
        self.nb_new_url = 0
        self.event_urls: dict[int, Event] = {}

    def load_cache(self):
        print("Making cache")
        l = (self.context.session.execute(select(Event)
             .where((Event.file_id == self.file.id) & (Event.url.is_(None))))
             .scalars().all())
        for e in l:
            self.event_urls[e.global_event_id] = e
            self.nb_ram += 1
        print(f"{self.nb_ram} objects in cache")

    def parse_row(self, row):
        e = self.mapper(row)
        if e.global_event_id in self.event_urls:
            self.nb_existing_event += 1
        else:
            url = self.url_mapper(row)
            if url is not None:
                e.url = url
                self.nb_new_url += 1

    def load(self, path: str, delimiter='\t', encoding="utf8", header=False):
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
        print("Committing")
        self.context.session.commit()
        self.duration(0.01)


if __name__ == '__main__':
    art.tprint(config.name, "big")
    print("Event Url Parser")
    print("================")
    print(f"V{config.version}")
    print(config.copyright)
    print()
    parser = argparse.ArgumentParser(description="Event Url Parser")
    parser.add_argument("path", help="Path")
    parser.add_argument("-e", "--echo", help="Sql Alchemy echo", action="store_true")
    parser.add_argument("-u", "--url", help="Ignore URL", action="store_true")
    args = parser.parse_args()
    context = Context()
    context.create(echo=args.echo)
    db_size = context.db_size() / 1024
    print(f"Database {context.db_name}: {db_size:.0f} Gb")
    p = EventUrlParser(context, args.url, config.nb_row_commit)
    p.load(args.path)
    print(f"New Urls: {p.nb_new_url}")
    new_db_size = context.db_size() / 1024
    print(f"Database {context.db_name}: {new_db_size:.0f} GB")
    print(f"Database grows: {new_db_size - db_size:.0f} MB ({((new_db_size - db_size) / db_size) * 100:.1f}%)")

    # NON TESTE