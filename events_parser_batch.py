import argparse
import datetime
from sqlalchemy import select, text, and_
from sqlalchemy.sql.operators import isnot
import config
import art
import base_parser
from dbcontext import Context
from event_parser import EventParser
from sqlentities import Event, File, Url
import time


class EventParserBatch: # Ne pas faire d'héritage car j'ai besoin de reinstancier à chaque iteration

    def __init__(self, context, ignore_url=False, nb_row_commit=config.nb_row_commit):
        self.context = context
        self.ignore_url = ignore_url
        self.nb_row_commit = nb_row_commit
        self.nb_file_to_import = 0
        self.nb_file_imported = 0
        self.nb_doublon = 0

    def parse_file(self, file: str):
        self.context.create_session()
        base_parser.time0 = time.perf_counter()
        db_size = context.db_size()
        p = EventParser(self.context, self.ignore_url, self.nb_row_commit)
        # print(f"Parsing {file}")
        p.load(f"{config.download_path}/{file}") # A terme mettre un try
        self.nb_file_imported += 1
        print(f"New Events: {p.nb_new_event}")
        print(f"Existing Events: {p.nb_existing_event}")
        new_db_size = context.db_size()
        print(f"Database {context.db_name}: {new_db_size:.0f} Mb")
        print(f"Database grows: {new_db_size - db_size:.0f} Mb ({((new_db_size - db_size) / db_size) * 100:.1f}%)")

    def parse(self):
        l = self.context.session.execute(
            select(File).where((File.dezip_date.isnot(None)) & (File.import_end_date.is_(None)))
            .order_by(File.dezip_date.desc())).scalars().all()
        id_min = 20150101  # 19000101
        id_max = 20151231  # 20991231
        for e in l:
            # print(e.name[:-4])
            if id_min <= e.id <= id_max:
                self.nb_file_to_import += 1
                self.parse_file(e.name[:-4])



    def remove_doublons(self):
        print("Check for doublons")
        sql = """select count(global_event_id) as nb, global_event_id from event
               group by global_event_id having count(global_event_id) > 1"""
        with self.context.engine.connect() as conn:
            res = conn.execute(text(sql))
            l = res.fetchall()
            for nb, gid in l:
                events = self.context.session.execute(select(Event).where(Event.global_event_id == gid)).scalars().all()
                for e in events[:-1]:
                    self.context.session.delete(e)
                    print(f"Delete event {e.global_event_id}")
                    self.nb_doublon += 1
            self.context.session.commit()


if __name__ == '__main__':
    art.tprint(config.name, "big")
    print("Events Parser Batch")
    print("===================")
    print(f"V{config.version}")
    print(config.copyright)
    print()
    parser = argparse.ArgumentParser(description="Events Parser Batch")
    parser.add_argument("-e", "--echo", help="Sql Alchemy echo", action="store_true")
    # parser.add_argument("-u", "--url", help="Ignore URL", action="store_true")
    args = parser.parse_args()
    context = Context()
    context.create(echo=args.echo)
    db_size = context.db_size()
    print(f"Database {context.db_name}: {db_size:.0f} Mb")
    p = EventParserBatch(context, True, config.nb_row_commit)
    p.parse()
    print(f"Nb file imported: {p.nb_file_imported}/{p.nb_file_to_import}")
    # p.remove_doublons()
    # print(f"Nb removed doublons: {p.nb_doublon}")
    new_db_size = context.db_size()
    print(f"Database {context.db_name}: {new_db_size:.0f} Mb")
    print(f"Database grows: {new_db_size - db_size:.0f} Mb ({((new_db_size - db_size) / db_size) * 100:.1f}%)")