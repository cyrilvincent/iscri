import argparse
import datetime
import config
import art
from base_parser import BaseParser
from dbcontext import Context
from sqlentities import Event, Url


class EventParser(BaseParser):

    def __init__(self, context, ignore_url=False, nb_row_commit=config.nb_row_commit):
        super().__init__(context)
        self.ignore_url = ignore_url
        self.nb_row_commit = nb_row_commit
        self.nb_existing_event = 0
        self.nb_new_event = 0
        # self.nb_new_actor = 0
        # self.nb_new_event_actor = 0
        # self.nb_new_geo = 0
        # self.nb_new_event_geo = 0
        self.actor_scores: dict[(str, str), int] = {}
        self.nb_actor_score = 0

    def mapper(self, row) -> Event:
        e = Event()
        try:
            e.global_event_id = self.get_int(row[0])
            e.date = self.get_date(row[1])
            # e.day = self.get_int((row[1]))
            e.month_year = self.get_int(row[2])
            e.year = self.get_int(row[3])
            if e.month_year is None or e.year is None:
                e.month_year = e.date.year * 100 + e.date.month
                e.year = e.date.year
            # e.fraction_date = self.get_float(row[4])
            i = 0
            e.actor1_code = self.get_str(row[5 + i])
            # e.actor1_is_gov = e.actor1_code is not None and "GOV" in e.actor1_code.upper()
            e.actor1_name = self.get_str(row[6 + i])
            e.actor1_country_code = self.get_str(row[7 + i])
            e.actor1_known_group_code = self.get_str(row[8 + i])
            e.actor1_ethnic_code = self.get_str(row[9 + i])
            e.actor1_religion1_code = self.get_str(row[10 + i])
            e.actor1_religion2_code = self.get_str(row[11 + i])
            e.actor1_type1_code = self.get_str(row[12 + i])
            e.actor1_type2_code = self.get_str(row[13 + i])
            e.actor1_type3_code = self.get_str(row[14 + i])
            i = 10
            e.actor2_code = self.get_str(row[5 + i])
            # e.actor2_is_gov = e.actor2_code is not None and "GOV" in e.actor2_code.upper()
            e.actor2_name = self.get_str(row[6 + i])
            e.actor2_country_code = self.get_str(row[7 + i])
            e.actor2_known_group_code = self.get_str(row[8 + i])
            e.actor2_ethnic_code = self.get_str(row[9 + i])
            e.actor2_religion1_code = self.get_str(row[10 + i])
            e.actor2_religion2_code = self.get_str(row[11 + i])
            e.actor2_type1_code = self.get_str(row[12 + i])
            e.actor2_type2_code = self.get_str(row[13 + i])
            e.actor2_type3_code = self.get_str(row[14 + i])

            e.is_root_event = self.get_bool(row[25])
            e.event_code = self.get_str(row[26])
            e.event_base_code = self.get_str(row[27])
            e.event_root_code = self.get_str(row[28])
            e.quad_class = self.get_int(row[29])
            e.goldstein_scale = self.get_float(row[30])
            e.num_mentions = self.get_int(row[31])
            e.num_sources = self.get_int(row[32])
            e.num_articles = self.get_int(row[33])
            # if e.quad_class + e.num_sources + e.num_articles + e.num_mentions > 32000:
            #     print(e.global_event_id)
            #     quit(99)
            e.avg_tone = self.get_float(row[34])
            # e.is_risk = e.goldstein_scale is not None and e.goldstein_scale < 0 and e.avg_tone < 0
            e.actor1_geo_type = self.get_int(row[35])
            e.actor1_geo_fullname = self.get_str(row[36])
            e.actor1_geo_country_code = self.get_str(row[37])
            e.actor1_geo_adm1_code = self.get_str(row[38])
            e.actor1_geo_lat = self.try_float(row[39])
            e.actor1_geo_lon = self.try_float(row[40])
            e.actor1_feature_id = self.get_str(row[41])
            e.actor2_geo_type = self.get_int(row[42])
            e.actor2_geo_fullname = self.get_str(row[43])
            e.actor2_geo_country_code = self.get_str(row[44])
            e.actor2_geo_adm1_code = self.get_str(row[45])
            e.actor2_geo_lat = self.try_float(row[46])
            e.actor2_geo_lon = self.try_float(row[47])
            e.actor2_feature_id = self.get_str(row[48])
            e.action_geo_type = self.get_int(row[49])
            e.action_geo_fullname = self.get_str(row[50])
            e.action_geo_country_code = self.get_str(row[51])
            e.action_geo_adm1_code = self.get_str(row[52])
            e.action_geo_lat = self.try_float(row[53])
            e.action_geo_lon = self.try_float(row[54])
            e.action_feature_id = self.get_str(row[55])
            e.date_added = self.get_date(row[56])
            e.parse_date = datetime.datetime.now()
        except Exception as ex:
            print(f"ERROR Event row {self.row_num} {e}\n{ex}\n{row}")
            quit(1)
        return e

    def url_mapper(self, row) -> Url:
        e = None
        if len(row) > 57:
            e = Url()
            e.url = self.get_str(row[57])
        return e

    def parse_row(self, row):
        e = self.mapper(row)
        if e.global_event_id in self.events:
            self.nb_existing_event += 1
        else:
            e.file = self.file
            e.gdelt_date = self.file.date
            self.events.add(e.global_event_id)
            self.nb_new_event += 1
            if not self.ignore_url:
                e.url = self.url_mapper(row)
            # self.parse_actors(e, row)
            # self.parse_events(e, row)
            self.context.session.add(e)
            # self.actor_scoring(e)
        if self.nb_new_event != 0 and self.nb_new_event % self.nb_row_commit == 0:
            if self.nb_row_commit >= 1000:
                print("Committing")
            self.context.session.commit()

    def post_load(self):
        self.file.import_end_date = datetime.datetime.now()


if __name__ == '__main__':
    art.tprint(config.name, "big")
    print("Event Parser")
    print("============")
    print(f"V{config.version}")
    print(config.copyright)
    print()
    parser = argparse.ArgumentParser(description="Event Parser")
    parser.add_argument("path", help="Path")
    parser.add_argument("-e", "--echo", help="Sql Alchemy echo", action="store_true")
    parser.add_argument("-u", "--url", help="Ignore URL", action="store_true")
    args = parser.parse_args()
    context = Context()
    context.create(echo=args.echo)
    db_size = context.db_size() / 1024
    print(f"Database {context.db_name}: {db_size:.0f} GB")
    p = EventParser(context, args.url, config.nb_row_commit)
    p.load(args.path)
    print(f"New Events: {p.nb_new_event}")
    print(f"Existing Events: {p.nb_existing_event}")
    new_db_size = context.db_size() / 1024
    print(f"Database {context.db_name}: {new_db_size:.0f} GB")
    print(f"Database grows: {new_db_size - db_size:.0f} MB ({((new_db_size - db_size) / db_size) * 100:.1f}%)")

    # -e data/20240921.small.CSV
    # data/20240921.export.CSV

    # 20240921 full 3568s 74Mo ?h ?j 270Go
    # w/o url 276s 56Mo 280h 12j 205Go
    # 2nd pass 321s 325h 14j
    # event only with cache 109s 16Mo 110h 5j
    # without cache idem
    # without commit row 12s 13h 0.5j
    # all except url : 37s 37h 1.5j

    # 20240922 full 238s 57Mo  241h 10j 210Go

    # 20170529 full 554s 133Mo 562h 24j  490Go

    # Flat w/o Url : 33s 31Mo 33h 1.5j 113Go
    # Flat all : 48s 46Mo 49h 2j 168Go
    # 2005 1710s (593s) NO RAM
    # 20130401 11s
    # 201303 1088s (311s) NO RAM La clé 253461390 est dupliquée dans 201303 et 20130401
    # 20160912 87s 35s

