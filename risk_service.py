import argparse
import datetime
from typing import Dict, Optional, Tuple
import art
from sqlalchemy import select, func
from sqlalchemy.orm import joinedload
from abc import ABCMeta, abstractmethod
import csv
import time
import calendar
import config
from dbcontext import Context
from sqlentities import File, Event, DailyRisk
import dateutil.relativedelta

class RiskService:

    def __init__(self, context):
        self.context = context
        self.daily_dict: dict[tuple[str, str], DailyRisk] = {}
        self.daily_set: set[datetime.date] = set()
        # self.score_set: set[tuple[int, int]] = set()
        self.nb_ram = 0
        self.nb_new_daily = 0

    def load_cache(self):
        print("Making cache")
        l = self.context.session.execute(select(DailyRisk.date)).scalars().all()
        for e in l:
            self.score_set.add(e)
            self.nb_ram += 1
        print(f"{self.nb_ram} objects in cache")

    def date_monthly_range(self):
        date = datetime.date(1979,1,1)
        end = datetime.date.today() - dateutil.relativedelta.relativedelta(months=1)
        end = datetime.date(end.year, end.month, calendar.monthrange(end.year, end.month)[1])
        while date < end:
            date = datetime.date(date.year, date.month, calendar.monthrange(date.year, date.month)[1])
            yield date
            date = date + dateutil.relativedelta.relativedelta(months=1)

    def date_range(self, start_date: datetime.date, end_date: datetime.date):
        days = int((end_date - start_date).days)
        for n in range(days):
            yield start_date + datetime.timedelta(n)

    def check_file_by_date(self, date: datetime.date):
        if date.year < 2007:
            e = self.context.session.execute(
                select(File).where((File.year == date.year) & (File.import_end_date.isnot(None)))).scalars().first()
            return e is not None
        if date.year < 2013 or (date.year == 2013 and date.month < 4):
            e = (self.context.session.execute(
                select(File)
                .where((File.year == date.year) & (File.month == date.month) & (File.import_end_date.isnot(None))))
                 .scalars().first())
            return e is not None
        l = (self.context.session.execute(
                select(File.id)
                .where((File.year == date.year) & (File.month == date.month) & (File.import_end_date.isnot(None))))
             .scalars().all())
        nb_not_in_html = 0
        if date.year == 2014 and date.month == 1:
            nb_not_in_html = 3
        elif date.year == 2014 and date.month == 3:
            nb_not_in_html = 1
        return len(l) == calendar.monthrange(date.year, date.month)[1] - nb_not_in_html
    # Verif également si le score_daily existe ou non
    # def compute(self):
    #     l: list[datetime.datetime] = self.context.session.execute(
    #         select(File.date).where(File.import_end_date.isnot(None))).scalars().all()
    #     for d in l:
    #         if (d.year, d.month) not in self.score_set:
    #             if self.check_file_by_date(d):
    #                 self.compute_month(d)
    #                 for k in self.score_dict:
    #                     e = IscriScore()
    #                     e.id = id
    #                     e.risk_date = datetime.datetime.now()
    #                     e.year = d.year
    #                     e.month = d.month
    #                     e.actor1_code = k[0]
    #                     e.actor2_code = k[1]
    #                     nb_month_day = calendar.monthrange(e.year, e.month)[1]
    #                     e.risk_score = self.score_dict[k] / nb_month_day
    #                     self.nb_new_score += 1
    #                     self.context.session.add(e)
    #                     print(e)
        # self.context.session.commit()

    #
    # def compute_monthly(self, d: datetime.date):
    #     print(f"Compute month {d.year}{d.month}")
    #     l: list[Event] = (self.context.session.execute(
    #         select(Event).
    #         where((Event.month_year == d.year * 100 + d.month) &
    #               (Event.actor1_is_gov) & (Event.actor2_is_gov) & (Event.is_root_event) & # à modifier par actor1_type1_code
    #               (Event.actor1_country_code.isnot(None)) & (Event.actor2_country_code.isnot(None)) &
    #               (Event.actor1_country_code != Event.actor2_country_code))) # C'est bien, on filtrera goldstein & avg_tone et quad dans la boucle
    #          .scalars().all())
    #     for e in l:
    #         key = e.actor1_geo_country_code, e.actor2_geo_country_code
    #         if key not in self.score_dict:
    #             self.score_dict[key] = 0
    #         self.score_dict[key] += 1

    def compute_daily(self, d: datetime.date):  # a appeler si d n'est pas dans daily_set
        print(f"Compute day {d}")
        l: list[Event] = self.context.session.execute(
            select(Event).
            where((Event.date == d) & Event.is_root_event &
                  ((Event.actor1_type1_code == "GOV") | (Event.actor2_type1_code == "GOV")) &
                  (Event.actor1_country_code.isnot(None)) & (Event.actor2_country_code.isnot(None)) &
                  (Event.actor1_country_code != Event.actor2_country_code))).scalars().all() # Filtrer sur File m+1 ?
        for e in l:
            key: tuple[str, str] = e.actor1_country_code, e.actor2_country_code
            if key not in self.daily_dict:
                self.daily_dict[key] = DailyRisk()
            self.daily_dict[key].total_nb += 1
            self.daily_dict[key].total_article_nb += e.num_articles
            if e.goldstein_scale < 0 and e.avg_tone < 0:
                if e.quad_class == 3:
                    self.daily_dict[key].quad3_nb += 1
                    self.daily_dict[key].article3_nb += e.num_articles
                elif e.quad_class == 4:
                    self.daily_dict[key].quad4_nb += 1
                    self.daily_dict[key].article4_nb += e.num_articles
            self.nb_new_daily += 1


if __name__ == '__main__':
    art.tprint(config.name, "big")
    print("Risk Manager")
    print("============")
    print(f"V{config.version}")
    print(config.copyright)
    print()
    parser = argparse.ArgumentParser(description="Event Parser")
    parser.add_argument("-e", "--echo", help="Sql Alchemy echo", action="store_true")
    parser.add_argument("-n", "--no_commit", help="No commit", action="store_true")
    args = parser.parse_args()
    context = Context()
    context.create(echo=args.echo)
    db_size = context.db_size()
    print(f"Database {context.db_name}: {db_size:.0f} Mb")
    m = RiskService(context)
    m.load_cache()
    m.compute()
    print(f"Nb new score: {m.nb_new_score}")
    new_db_size = context.db_size()
    print(f"Database {context.db_name}: {new_db_size:.0f} Mb")
    print(f"Database grows: {new_db_size - db_size:.0f} Mb ({((new_db_size - db_size) / db_size) * 100:.1f}%)")