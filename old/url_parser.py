import argparse
import datetime
from sqlalchemy import select, text
import config
import art
import base_parser
import time
from dbcontext import Context
from event_parser import EventParser
from sqlentities import Event, File, Url #, Actor, EventActor, Geo, EventGeo



class EventUrlParser(EventParser):

    def __init__(self, context, nb_row_commit=config.nb_row_commit):
        super().__init__(context, False, nb_row_commit)
        self.nb_new_url = 0

    def load_cache(self):
        pass

    def parse_row(self, row):
        e = self.mapper(row)
        event = (self.context.session.execute(select(Event).where(Event.global_event_id == e.global_event_id))
                 .scalars().first())
        if event.url_id is None:
            event.url = self.url_mapper(row)
            self.context.session.add(e)
            if self.nb_new_url != 0 and self.nb_new_url % self.nb_row_commit == 0:
                if self.nb_row_commit >= 1000:
                    print("Committing")
                self.context.session.commit()

    def post_load(self):
        pass

    def parse_file(self, file: str):
        p = EventUrlParser(self.context, self.nb_row_commit)
        p.load(f"{config.download_path}/{file}")

    def parse_all(self):
        l = self.context.session.execute(
            select(File).where((File.dezip_date.isnot(None)) & (File.import_end_date.is_(None)))
            .order_by(File.dezip_date.desc())).scalars().all()
        id_min = 20130401
        id_max = 20991231
        for e in l:
            id = e.id
            if id_min <= id <= id_max:
                name: str = e.name[:-4] + "csv"
                p.load(f"{config.download_path}/{name}")


if __name__ == '__main__':
    art.tprint(config.name, "big")
    print("Event URL Parser")
    print("================")
    print(f"V{config.version}")
    print(config.copyright)
    print()
    parser = argparse.ArgumentParser(description="Event URL Parser")
    parser.add_argument("-e", "--echo", help="Sql Alchemy echo", action="store_true")
    args = parser.parse_args()
    context = Context()
    context.create(echo=args.echo)
    db_size = context.db_size() / 1024
    print(f"Database {context.db_name}: {db_size:.0f} Gb")
    p = EventUrlParser(context, config.nb_row_commit)
    # p.load(args.path)
    p.parse_all()
    print(f"New URLs: {p.nb_new_url}")
    new_db_size = context.db_size() / 1024
    print(f"Database {context.db_name}: {new_db_size:.0f} Gb")
    print(f"Database grows: {new_db_size - db_size:.0f} Mb ({((new_db_size - db_size) / db_size) * 100:.1f}%)")
