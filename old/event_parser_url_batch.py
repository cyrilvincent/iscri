import argparse
from sqlalchemy import select
import config
import art
import base_parser
from dbcontext import Context
from old.event_parser_url import EventUrlParser
from sqlentities import File
import time


class EventUrlParserBatch:

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
        p = EventUrlParser(self.context, self.ignore_url, self.nb_row_commit)
        p.load(f"{config.download_path}/{file}")
        self.nb_file_imported += 1
        print(f"New Urls: {p.nb_new_url}")
        new_db_size = context.db_size()
        print(f"Database {context.db_name}: {new_db_size:.0f} MB")
        print(f"Database grows: {new_db_size - db_size:.0f} MB ({((new_db_size - db_size) / db_size) * 100:.1f}%)")

    def parse(self):
        l = self.context.session.execute(
            select(File).where((File.dezip_date.isnot(None)) & (File.import_end_date.is_(None)))
            .order_by(File.dezip_date)).scalars().all()
        id_min = 20130401
        id_max = 20991231
        for e in l:
            id = e.id
            if id_min <= id <= id_max:
                self.nb_file_to_import += 1
                name: str = e.name[:-4] if "export" in e.name else e.name[:-3] + "csv"
                self.parse_file(name)


if __name__ == '__main__':
    art.tprint(config.name, "big")
    print("Events URL Parser Batch")
    print("=======================")
    print(f"V{config.version}")
    print(config.copyright)
    print()
    parser = argparse.ArgumentParser(description="Events URL Parser Batch")
    parser.add_argument("-e", "--echo", help="Sql Alchemy echo", action="store_true")
    parser.add_argument("-u", "--url", help="Ignore URL", action="store_true")
    args = parser.parse_args()
    context = Context()
    context.create(echo=args.echo)
    db_size = context.db_size()
    print(f"Database {context.db_name}: {db_size:.0f} MB")
    p = EventUrlParserBatch(context, args.url, config.nb_row_commit)
    p.parse()
    print(f"Nb file imported: {p.nb_file_imported}/{p.nb_file_to_import}")
    new_db_size = context.db_size()
    print(f"Database {context.db_name}: {new_db_size:.0f} MB")
    print(f"Database grows: {new_db_size - db_size:.0f} MB ({((new_db_size - db_size) / db_size) * 100:.1f}%)")
