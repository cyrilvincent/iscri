import argparse
import datetime

from sqlalchemy import select

import config
import art
from base_parser import BaseParser
from dbcontext import Context
from sqlentities import Event, File, Url, ActorScore, Actor, EventActor, Geo, EventGeo


class EventParser(BaseParser):

    def __init__(self, context):
        super().__init__(context)
        self.nb_existing_event = 0
        self.nb_new_event = 0
        self.nb_new_actor = 0
        self.nb_new_event_actor = 0
        self.nb_new_geo = 0
        self.nb_new_event_geo = 0
        self.actor_scores: dict[(str, str), int] = {}
        self.nb_actor_score = 0

    def mapper(self, row) -> Event:
        e = Event()
        try:
            e.id = self.get_int(row[0])
            e.date = self.get_date(row[1])
            e.day = self.get_int((row[1]))
            e.month_year = self.get_int(row[2])
            e.year = self.get_int(row[3])
            e.fraction_date = self.get_float(row[4])
            # i = 0
            # e.actor1_code = self.get_str(row[5 + i])
            # e.actor1_name = self.get_str(row[6 + i])
            # e.actor1_country_code = self.get_str(row[7 + i])
            # e.actor1_known_group_code = self.get_str(row[8 + i])
            # e.actor1_ethnic_code = self.get_str(row[9 + i])
            # e.actor1_religion1_code = self.get_str(row[10 + i])
            # e.actor1_religion2_code = self.get_str(row[11 + i])
            # e.actor1_type1_code = self.get_str(row[12 + i])
            # e.actor1_type2_code = self.get_str(row[13 + i])
            # e.actor1_type3_code = self.get_str(row[14 + i])
            # i = 10
            # e.actor2_code = self.get_str(row[5 + i])
            # e.actor2_name = self.get_str(row[6 + i])
            # e.actor2_country_code = self.get_str(row[7 + i])
            # e.actor2_known_group_code = self.get_str(row[8 + i])
            # e.actor2_ethnic_code = self.get_str(row[9 + i])
            # e.actor2_religion1_code = self.get_str(row[10 + i])
            # e.actor2_religion2_code = self.get_str(row[11 + i])
            # e.actor2_type1_code = self.get_str(row[12 + i])
            # e.actor2_type2_code = self.get_str(row[13 + i])
            # e.actor2_type3_code = self.get_str(row[14 + i])
            e.is_root_event = self.get_int(row[25])
            e.event_code = self.get_str(row[26])
            e.event_base_code = self.get_str(row[27])
            e.event_root_code = self.get_str(row[28])
            e.quad_class = self.get_int(row[29])
            e.goldstein_scale = self.get_float(row[30])
            e.num_mentions = self.get_int(row[31])
            e.num_sources = self.get_int(row[32])
            e.num_articles = self.get_int(row[33])
            e.avg_tone = self.get_float(row[34])
            e.actor1_geo_type = self.get_int(row[35])
            e.actor1_geo_fullname = self.get_str(row[36])
            e.actor1_geo_country_code = self.get_str(row[37])
            e.actor1_geo_adm1_code = self.get_str(row[38])
            e.actor1_geo_lat = self.get_float(row[39])
            e.actor1_geo_lon = self.get_float(row[40])
            e.actor1_feature_id = self.get_str(row[41])
            e.actor2_geo_type = self.get_int(row[42])
            e.actor2_geo_fullname = self.get_str(row[43])
            e.actor2_geo_country_code = self.get_str(row[44])
            e.actor2_geo_adm1_code = self.get_str(row[45])
            e.actor2_geo_lat = self.get_float(row[46])
            e.actor2_geo_lon = self.get_float(row[47])
            e.actor2_feature_id = self.get_str(row[48])
            e.action_geo_type = self.get_int(row[49])
            e.action_geo_fullname = self.get_str(row[50])
            e.action_geo_country_code = self.get_str(row[51])
            e.action_geo_adm1_code = self.get_str(row[52])
            e.action_geo_lat = self.get_float(row[53])
            e.action_geo_lon = self.get_float(row[54])
            e.action_feature_id = self.get_str(row[55])
            e.date_added = self.get_int(row[56])
            e.parse_date = datetime.datetime.now()
        except Exception as ex:
            print(f"ERROR Event row {self.row_num} {e}\n{ex}\n{row}")
            quit(1)
        return e

    def actor_mapper(self, row, num_actor: int) -> Actor | None:
        e = Actor()
        try:
            i = 0 if num_actor == 1 else 10
            e.code = self.get_str(row[5 + i])
            if e.code is None:
                return None
            e.name = self.get_str(row[6 + i])
            e.country_code = self.get_str(row[7 + i])
            e.known_group_code = self.get_str(row[8 + i])
            e.ethnic_code = self.get_str(row[9 + i])
            e.religion1_code = self.get_str(row[10 + i])
            e.religion2_code = self.get_str(row[11 + i])
            e.type1_code = self.get_str(row[12 + i])
            e.type2_code = self.get_str(row[13 + i])
            e.type3_code = self.get_str(row[14 + i])
            e.actor1_geo_type = self.get_int(row[35])
            e.actor1_geo_fullname = self.get_str(row[36])
            e.actor1_geo_country_code = self.get_str(row[37])
            e.actor1_geo_adm1_code = self.get_str(row[38])
            e.actor1_geo_lat = self.get_float(row[39])
            e.actor1_geo_lon = self.get_float(row[40])
            e.actor1_feature_id = self.get_str(row[41])
            e.parse_date = datetime.datetime.now()
        except Exception as ex:
            print(f"ERROR Actor row {self.row_num} {e}\n{ex}\n{row}")
            quit(2)
        return e

    def geo_mapper(self, row, num_geo: int) -> Geo | None:
        e = Geo()
        try:
            i = (num_geo - 1) * 7
            e.type = self.get_int(row[35 + i])
            if e.type is None:
                return None
            e.fullname = self.get_str(row[36 + i])
            e.country_code = self.get_str(row[37 + i])
            e.adm1_code = self.get_str(row[38 + i])
            e.lat = self.get_float(row[39 + i])
            e.lon = self.get_float(row[40 + i])
            e.feature_id = self.get_str(row[41 + i])
            e.parse_date = datetime.datetime.now()
        except Exception as ex:
            print(f"ERROR Geo row {self.row_num} {e}\n{ex}\n{row}")
            quit(3)
        return e

    def url_mapper(self, row) -> Url:
        e = None
        if len(row) >= 57:
            e = Url()
            e.url = self.get_str(row[57])
        return e

    def actor_scoring(self, e: Event):
        if e.actor1_code is not None and e.actor2_code is not None:
            key = e.actor1_code, e.actor2_code
            if e.actor1_code > e.actor2_code:
                key = e.actor2_code, e.actor1_code
            if key not in self.actor_scores:
                self.actor_scores[key] = 0
            self.actor_scores[key] += 1

    def save_actor_scores(self):
        print(f"Saving {len(self.actor_scores)} actor scores")
        for k in self.actor_scores.keys():
            e = (self.context.session.execute(select(ActorScore)
                 .where((ActorScore.actor1_code == k[0]) & (ActorScore.actor2_code == k[1])))
                 .scalars().first())
            if e is None:
                e = ActorScore()
                e.actor1_code = k[0]
                e.actor2_code = k[1]
                e.score = 0
                e.last_update = 0
                self.context.session.add(e)
            if e.last_update < self.file.date:
                e.last_update = self.file.date
                e.score += self.actor_scores[k]
                self.nb_actor_score += 1
                self.context.session.commit()

    def parse_actors(self, e: Event, row):
        for num in [1, 2]:
            a = self.actor_mapper(row, num)
            if a is not None:
                if a.key not in self.actors:
                    self.actors[a.key] = a
                    self.nb_new_actor += 1
                else:
                    a = self.actors[a.key]
                ea = EventActor()
                ea.event = e
                ea.actor = a
                ea.num = num
                self.nb_new_event_actor += 1

    def parse_events(self, e: Event, row):
        for num in [1, 2, 3]:
            g = self.geo_mapper(row, num)
            if g is not None:
                if g.key not in self.geos:
                    self.geos[g.key] = g
                    self.nb_new_event += 1
                else:
                    g = self.geos[g.key]
                eg = EventGeo()
                eg.event = e
                eg.geo = g
                eg.num = num
                self.nb_new_event_geo += 1

    def parse_row(self, row):
        e = self.mapper(row)
        if e.id in self.events:
            self.nb_existing_event += 1
            e = self.events[e.id]
            if e.file_id != self.file.id:
                e.file = self.file
        else:
            e.file = self.file
            self.events[e.id] = e
            self.nb_new_event += 1
            e.url = self.url_mapper(row)
            self.parse_actors(e, row)
            self.parse_events(e, row)
            self.context.session.add(e)
            # self.actor_scoring(e)
        self.context.session.commit()

    # def post_load(self):
    #     self.save_actor_scores()


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
    args = parser.parse_args()
    context = Context()
    context.create(echo=args.echo)
    db_size = context.db_size()
    print(f"Database {context.db_name}: {db_size:.0f} Mb")
    p = EventParser(context)
    p.load(args.path)
    print(f"New File: {p.is_new_file}")
    print(f"New Events: {p.nb_new_event}")
    print(f"Existing Events: {p.nb_existing_event}")
    print(f"New Actors: {p.nb_new_actor}")
    print(f"New Event-Actor: {p.nb_new_event_actor}")
    new_db_size = context.db_size()
    print(f"Database {context.db_name}: {new_db_size:.0f} Mb")
    print(f"Database grows: {new_db_size - db_size:.0f} Mb ({((new_db_size - db_size) / db_size) * 100:.1f}%)")

    # -e data/20240921.small.CSV
    # data/20240921.export.CSV


