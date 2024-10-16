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
from sqlentities import File, Event, DailyRisk, Iscri
import dateutil.relativedelta
import country_converter as coco


class RiskService:

    def __init__(self, context):
        self.context = context
        self.daily_set: set[datetime.date] = set()
        self.monthly_set: set[tuple[int, int]] = set()
        self.nb_ram = 0
        self.nb_new_daily = 0
        self.nb_new_monthly = 0
        self.cc = coco.CountryConverter()

    # def load_cache(self):
    #     print("Making cache")
    #     l = self.context.session.execute(select(DailyRisk.date)).scalars().all()
    #     for e in l:
    #         self.score_set.add(e)
    #         self.nb_ram += 1
    #     print(f"{self.nb_ram} objects in cache")

    def last_day_of_month(self, year: int, month: int) -> int:
        return calendar.monthrange(year, month)[1]

    def month_range(self, start_date: datetime.date, end_date: datetime.date):
        end = datetime.date(end_date.year, end_date.month, self.last_day_of_month(end_date.year, end_date.month))
        date = start_date
        while date < end:
            date = datetime.date(date.year, date.month, self.last_day_of_month(date.year, date.month))
            yield date
            date = date + dateutil.relativedelta.relativedelta(months=1)

    def date_range(self, start_date: datetime.date, end_date: datetime.date):
        days = int((end_date - start_date).days)
        for n in range(days):
            yield start_date + datetime.timedelta(n)

    def is_all_files_presents_by_year_month(self, year: int, month: int):
        if year < 2007:
            e = (self.context.session.execute(
                select(File).where((File.date <= datetime.date(2007, 12, 1)) & (File.import_end_date.isnot(None))))
                 .scalars().first())
            return e is not None
        if year < 2013 or (year == 2013 and month < 4):
            e = (self.context.session.execute(
                select(File)
                .where((File.date == datetime.date(year, month, 1)) &
                       (File.import_end_date.isnot(None))))
                 .scalars().first())
            return e is not None
        l = (self.context.session.execute(
                select(File.id)
                .where((File.date >= datetime.date(year, month, 1)) &
                       (File.date <= datetime.date(year, month, self.last_day_of_month(year, month))) &
                       (File.import_end_date.isnot(None))))
             .scalars().all())
        nb_not_in_html = 0
        if year == 2014 and month == 1:
            nb_not_in_html = 3
        elif year == 2014 and month == 3:
            nb_not_in_html = 1
        return len(l) == self.last_day_of_month(year, month) - nb_not_in_html

    def norm_country_code(self, code: str) -> str:
        if len(code) == 3:
            if code in self.cc.data.ISO3.values:
                return code
            if code in self.cc.data.IOC:
                res = self.cc.convert([code], src="IOC", to="ISO3", enforce_list=True, not_found=None)[0]
                if len(res) != 0:
                    return res[0]
        elif len(code) == 2:
            if code in self.cc.data.ISO2:
                res = self.cc.convert([code], src="ISO2", to="ISO3", enforce_list=True, not_found=None)[0]
                if len(res) != 0:
                    return res[0]
        res = self.cc.convert([code], to="ISO3", enforce_list=True, not_found=None)[0]
        if len(res) != 0:
            return res[0]
        return code



    def compute_daily(self, d: datetime.date) -> dict[tuple[str, str], DailyRisk] | None:
        daily_dict: dict[tuple[str, str], DailyRisk] = {}
        l: list[Event] = self.context.session.execute(
            select(Event).
            where((Event.date == d) & Event.is_root_event &
                  ((Event.actor1_type1_code == "GOV") | (Event.actor2_type1_code == "GOV")) &
                  (Event.actor1_country_code.isnot(None)) & (Event.actor2_country_code.isnot(None)) &
                  (Event.actor1_country_code != Event.actor2_country_code))).scalars().all() # Filtrer sur File m+1 ?
        first = True
        for e in l:
            # code1 = self.norm_country_code(e.actor1_country_code)
            # code2 = self.norm_country_code(e.actor2_country_code)
            key: tuple[str, str] = e.actor1_country_code, e.actor2_country_code
            if key not in daily_dict:
                first = True
                if first:
                    print(f"Compute daily risk {d}")
                    first = False
                dr = DailyRisk()
                dr.total_nb = dr.total_article_nb = dr.quad3_nb = dr.quad4_nb = dr.article3_nb = dr.article4_nb = 0
                dr.actor1_code, dr.actor2_code = key
                dr.date = d
                dr.compute_date = datetime.datetime.now()
                daily_dict[key] = dr
            daily_dict[key].total_nb += 1
            daily_dict[key].total_article_nb += e.num_articles
            if (e.goldstein_scale is None or e.goldstein_scale < 0) and e.avg_tone < 0:
                if e.quad_class == 3:
                    daily_dict[key].quad3_nb += 1
                    daily_dict[key].article3_nb += e.num_articles
                elif e.quad_class == 4:
                    daily_dict[key].quad4_nb += 1
                    daily_dict[key].article4_nb += e.num_articles
        return daily_dict

    def compute_dailies(self, start_date=datetime.date(1979, 1, 1), end_date=datetime.date.today()):
        print("Compute daily risks") # On devrait recalculer les risques même 1 mois après
        l: list[datetime.date] = self.context.session.execute(
            select(DailyRisk.date).where((DailyRisk.date >= start_date) & (DailyRisk.date <= end_date))
            .distinct()).scalars().all()
        for e in l:
            self.daily_set.add(e)
        previous = start_date.month
        for d in self.date_range(start_date, end_date + datetime.timedelta(days=1)):
            if d not in self.daily_set:
                dico = self.compute_daily(d)
                for dr in dico.values():
                    if dr.quad4_nb + dr.quad3_nb != 0:
                        self.context.session.add(dr)
                        self.nb_new_daily += 1
                if d.month != previous:
                    self.context.session.commit()
                    previous = d.month
        self.context.session.commit()

    def compute_monthly(self, last_month_day: datetime.date) -> dict[tuple[str, str], Iscri]:
        print(f"Compute month {last_month_day.year}-{last_month_day.month:02d}")
        first_month_day = datetime.date(year=last_month_day.year, month=last_month_day.month, day=1)
        l: list[DailyRisk] = self.context.session.execute(
            select(DailyRisk).
            where((DailyRisk.date >= first_month_day) & (DailyRisk.date <= last_month_day))).scalars().all()
        dico: dict[tuple[str, str], Iscri] = {}
        for e in l:
            if (e.actor1_code, e.actor2_code) not in dico:
                i = Iscri()
                i.actor1_code, i.actor2_code = e.actor1_code, e.actor2_code
                i.year, i.month = last_month_day.year, last_month_day.month
                i.risk_date = datetime.datetime.now()
                i.risk = i.risk3 = i.risk4 = 0
                dico[e.actor1_code, e.actor2_code] = i
            i = dico[(e.actor1_code, e.actor2_code)]
            i.risk += ((e.quad3_nb + e.quad4_nb) / e.total_nb) / last_month_day.day
            i.risk3 += (e.quad3_nb / e.total_nb) / last_month_day.day
            i.risk4 += (e.quad4_nb / e.total_nb) / last_month_day.day
        return dico

    def compute_monthlies(self, start_date=datetime.date(1979, 1, 1), end_date=datetime.date.today()):
        print("Compute monthly risks")
        l: list[Iscri] = self.context.session.execute(
            select(Iscri)
            .where((Iscri.year * 100 + Iscri.month >= start_date.year * 100 + start_date.month) &
                   (Iscri.year * 100 + Iscri.month <= end_date.year * 100 + end_date.month))
            ).scalars().all()
        for e in l:
            if (e.year, e.month) not in self.monthly_set:
                self.monthly_set.add((e.year, e.month))
        for m in self.month_range(start_date, end_date):
            if (m.year, m.month) not in self.monthly_set:  # On devrait pouvoir recalculer le mois en cours
                if self.is_all_files_presents_by_year_month(m.year, m.month):  # sauf pour le mois en cours
                    dico = self.compute_monthly(m)
                    for i in dico.values():
                        if i.risk != 0:
                            self.context.session.add(i)
                            self.nb_new_monthly += 1
                            self.context.session.commit()
                else:
                    print(f"Month {m.year}-{m.month:02d} is not complete")

    def iscri(self, risk: float, previous: float) -> float:
        return risk + 0.9 * previous

    def compute_iscri(self, i: Iscri, previous: Iscri | None) -> Iscri:
        if previous is None:
            previous = Iscri()
            previous.iscri3 = previous.iscri4 = previous.iscri = 0
        i.iscri3 = self.iscri(i.risk3, previous.iscri3)
        i.iscri4 = self.iscri(i.risk4, previous.iscri4)
        i.iscri = self.iscri(i.risk, previous.iscri)
        i.iscri_date = datetime.datetime.now()
        return i  # A tester avec previous = None

    def compute_iscri_monthly(self, year: int, month: int):
        print(f"Compute iscri for {year}-{month:02d}")
        previous_year, previous_month = year, month - 1
        if previous_month == 0:
            previous_year, previous_month = previous_year - 1, 12
        dico: dict[tuple[str, str], tuple[Iscri, Iscri | None]] = {}
        l = self.context.session.execute(
            select(Iscri).where((Iscri.year == year) & (Iscri.month == month))).scalars.all()
        for e in l:
            dico[e.actor1_code, e.actor2_code] = e, None
        l = self.context.session.execute(
            select(Iscri).where((Iscri.year == previous_year) & (Iscri.month == previous_month))).scalars.all()
        for e in l:
            if (e.actor1_code, e.actor2_code) not in dico:
                i = Iscri()
                i.risk3 = i.risk4 = i.risk = 0
                i.actor1_code, i.actor2_code = e.actor1_code, e.actor2_code
                i.year, i.month = year, month
                i.risk_date = datetime.datetime.now()
                dico[e.actor1_code, e.actor2_code] = i, e
                self.context.session.add(i)
            else:
                dico[e.actor1_code, e.actor2_code] = dico[e.actor1_code, e.actor2_code][0], e
        for t in dico.values():
            self.compute_iscri(t[0], t[1])
        self.context.session.commit()







    def compute_iscri_monthlies(self, start_date=datetime.date(1979, 1, 1), end_date=datetime.date.today()):
        print("Compute iscris")
        for m in self.month_range(start_date, end_date):
            iscris: list[Iscri] = self.context.session.execute(
                select(Iscri).where((Iscri.year == m.year) & (Iscri.month == m.month))).scalars.all()
            previouss = None # A faire avec le mois précédent m.addmonth(-1)
            # previous_dico todo
            #Faire une boucle par pair de pays
            for iscri in iscris:
                if iscri is None: # A remonter d'un cran len(iscris)==0
                    print(f"Stop at {m.year}-{m.month:02d}")
                    break
                # A porter dans compute_iscri_monthly
                elif iscri.iscri_date is None:
                    print(f"Compute iscri for {m.year}-{m.month:02d}")
                    # Créer les iscri pour chaque pair
                    # le current peut être à None ainsi que le previous
                    # filtrer si iscri < 1e-5
                    # Appeler le monthly uniquement si l'iscri est None
                else:
                    print(f"Iscri is already computed for {m.year}-{m.month:02d}")
                # previous = i


    def modify_iscri(self, year: int, month: int, actor1_code: str, actor2_code: str,
                     iscri: float, iscri3=0.0, iscri4=0.0):
        i = (self.context.session.execute(
            select(Iscri).where((Iscri.year == year) & (Iscri.month == month) &
                                (Iscri.actor1_code == actor1_code) & (Iscri.actor2_code == actor2_code))).
             scalars().first())
        i.iscri, i.iscri3, i.iscri4 = iscri, iscri3, iscri4
        self.context.session.commit()









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
    m.compute_dailies(datetime.date(2015, 1, 1), datetime.date(2015, 12, 31))
    m.compute_monthlies(datetime.date(2015, 2, 1), datetime.date(2015, 12, 31))
    print(f"Nb new daily risks: {m.nb_new_daily}")
    print(f"Nb new monthly risks: {m.nb_new_monthly}")
    new_db_size = context.db_size()
    print(f"Database {context.db_name}: {new_db_size:.0f} Mb")
    print(f"Database grows: {new_db_size - db_size:.0f} Mb ({((new_db_size - db_size) / db_size) * 100:.1f}%)")
