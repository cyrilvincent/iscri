import argparse
import datetime
from typing import Dict, Optional, Tuple
import art
from sqlalchemy import select, func
import calendar
import config
from dbcontext import Context
from sqlentities import File, Event, DailyRisk, Iscri
import dateutil.relativedelta

class RiskService:

    def __init__(self, context):
        self.context = context
        self.daily_set: set[datetime.date] = set()
        self.monthly_set: set[tuple[int, int]] = set()
        self.nb_ram = 0
        self.nb_new_daily = 0
        self.nb_new_monthly = 0
        self.nb_new_iscri = 0
        self.iscri_acceleration = 0.9

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
        if year < 2006:
            e = (self.context.session.execute(
                select(File)
                .where((File.date == datetime.date(year, 12, 31)) & (File.import_end_date.isnot(None))))
                 .scalars().first())
            return e is not None
        if year < 2013 or (year == 2013 and month < 4):
            e = (self.context.session.execute(
                select(File)
                .where((File.date == datetime.date(year, month, 1)) & (File.import_end_date.isnot(None))))
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
        elif year == 2022 and month == 11:
            nb_not_in_html = 1
        elif year == 2023 and month == 3:
            nb_not_in_html = 2
        res = len(l) >= self.last_day_of_month(year, month) - nb_not_in_html
        if res is False:
            pass
        return res

    def compute_daily(self, d: datetime.date) -> dict[tuple[str, str], DailyRisk] | None:
        daily_dict: dict[tuple[str, str], DailyRisk] = {}
        l: list[Event] = self.context.session.execute(
            select(Event).
            where((Event.date == d) & Event.is_root_event &
                  ((Event.actor1_type1_code == "GOV") | (Event.actor2_type1_code == "GOV")) &
                  (Event.actor1_country_code.isnot(None)) & (Event.actor2_country_code.isnot(None)) # &
                  # (Event.actor1_country_code != Event.actor2_country_code))
                  )).scalars().all() # Filtrer sur File m+1 ?
        first = True
        for e in l:
            # code1 = self.norm_country_code(e.actor1_country_code)
            # code2 = self.norm_country_code(e.actor2_country_code)
            key: tuple[str, str] = e.actor1_country_code, e.actor2_country_code
            if key not in daily_dict:
                if first:
                    print(f"Compute daily risk {d}")
                    first = False
                dr = DailyRisk()
                # dr.total_nb = dr.total_article_nb = dr.quad3_nb = dr.quad4_nb = dr.article3_nb = dr.article4_nb = 0
                dr.total_nb = dr.quad3_nb = dr.quad4_nb = 0
                dr.goldstein_quad3_nb = dr.goldstein_quad4_nb = dr.goldstein_nb = 0
                dr.actor1_code, dr.actor2_code = key
                dr.date = d
                dr.compute_date = datetime.datetime.now()
                daily_dict[key] = dr
            daily_dict[key].total_nb += 1
            if e.quad_class == 3:
                daily_dict[key].quad3_nb += 1
            elif e.quad_class == 4:
                daily_dict[key].quad4_nb += 1
            if e.goldstein_scale is not None and e.goldstein_scale < 0:
                daily_dict[key].goldstein_nb += 1
                if e.quad_class == 3:
                    daily_dict[key].goldstein_quad3_nb += 1
                elif e.quad_class == 4:
                    daily_dict[key].goldstein_quad4_nb += 1

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
        print(f"Compute risk month {last_month_day.year}-{last_month_day.month:02d}")
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
                i.risk_g = i.risk_g3 = i.risk_g4 = i.risk_g34 = 0
                dico[e.actor1_code, e.actor2_code] = i
            i = dico[(e.actor1_code, e.actor2_code)]
            if e.total_nb != 0:
                i.risk += ((e.quad3_nb + e.quad4_nb) / e.total_nb) / last_month_day.day
                i.risk3 += (e.quad3_nb / e.total_nb) / last_month_day.day
                i.risk4 += (e.quad4_nb / e.total_nb) / last_month_day.day
                i.risk_g34 += ((e.goldstein_quad3_nb + e.goldstein_quad4_nb) / e.total_nb) / last_month_day.day
                i.risk_g3 += (e.goldstein_quad3_nb / e.total_nb) / last_month_day.day
                i.risk_g4 += (e.goldstein_quad4_nb / e.total_nb) / last_month_day.day
                i.risk_g += (e.goldstein_nb / e.total_nb) / last_month_day.day
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
            if (m.year, m.month) not in self.monthly_set:
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
        return risk + self.iscri_acceleration * previous

    def compute_iscri(self, i: Iscri, previous: Iscri | None) -> Iscri:
        if previous is None:
            previous = Iscri()
            previous.iscri3 = previous.iscri4 = previous.iscri = 0
            previous.iscri_g34 = previous.iscri_g3 = previous.iscri_g4 = previous.iscri_g = 0
        i.iscri3 = self.iscri(i.risk3, previous.iscri3)
        i.iscri4 = self.iscri(i.risk4, previous.iscri4)
        i.iscri = self.iscri(i.risk, previous.iscri)
        i.iscri_g3 = self.iscri(i.risk_g3, previous.iscri_g3)
        i.iscri_g4 = self.iscri(i.risk_g4, previous.iscri_g4)
        i.iscri_g34 = self.iscri(i.risk_g34, previous.iscri_g34)
        i.iscri_g = self.iscri(i.risk_g, previous.iscri_g)
        i.iscri_date = datetime.datetime.now()
        return i

    def compute_iscri_monthly(self, year: int, month: int):
        print(f"Compute iscri month {year}-{month:02d}")
        previous_year, previous_month = year, month - 1
        if previous_month == 0:
            previous_year, previous_month = previous_year - 1, 12
        dico: dict[tuple[str, str], tuple[Iscri, Iscri | None]] = {}
        l = self.context.session.execute(
            select(Iscri).where((Iscri.year == year) & (Iscri.month == month))).scalars().all()
        for e in l:
            dico[e.actor1_code, e.actor2_code] = e, None
        l = self.context.session.execute(
            select(Iscri).where((Iscri.year == previous_year) & (Iscri.month == previous_month) &
                                (Iscri.iscri > 1e-5))).scalars().all()
        for e in l:
            if e.actor1_code != e.actor2_code:
                if (e.actor1_code, e.actor2_code) not in dico:
                    i = Iscri()
                    i.risk3 = i.risk4 = i.risk = 0  # # For iscri when no risk in the current month
                    i.risk_g34 = i.risk_g3 = i.risk_g4 = i.risk_g = 0
                    i.actor1_code, i.actor2_code = e.actor1_code, e.actor2_code
                    i.year, i.month = year, month
                    i.risk_date = datetime.datetime.now()
                    dico[e.actor1_code, e.actor2_code] = i, e
                    self.context.session.add(i)
                    self.nb_new_iscri += 1
                else:
                    dico[e.actor1_code, e.actor2_code] = dico[e.actor1_code, e.actor2_code][0], e
        for t in dico.values():
            self.compute_iscri(t[0], t[1])
        self.context.session.commit()

    def compute_iscri_monthlies(self, start_date=datetime.date(1979, 1, 1), end_date=datetime.date.today()):
        print("Compute iscris")
        for m in self.month_range(start_date, end_date):
            iscri: Iscri = self.context.session.execute(
                select(Iscri).where((Iscri.year == m.year) & (Iscri.month == m.month))).scalars().first()
            if iscri is None:
                print(f"Stop at {m.year}-{m.month:02d}")
                break
            if iscri.iscri_date is None:
                self.compute_iscri_monthly(m.year, m.month)
            else:
                print(f"Iscri is already computed for {m.year}-{m.month:02d}")

    def update_iscri(self, year: int, month: int, actor1_code: str, actor2_code: str,
                     iscri: float, iscri3=0.0, iscri4=0.0):
        i = (self.context.session.execute(
            select(Iscri).where((Iscri.year == year) & (Iscri.month == month) &
                                (Iscri.actor1_code == actor1_code) & (Iscri.actor2_code == actor2_code))).
             scalars().first())
        i.iscri, i.iscri3, i.iscri4 = iscri, iscri3, iscri4
        i.iscri_date = datetime.datetime.now()
        self.context.session.commit()

    def create_iscri(self, year: int, month: int, actor1_code: str, actor2_code: str,
                     iscri: float, iscri3=0.0, iscri4=0.0):
        i = Iscri()
        i.actor1_code, i.actor2_code = actor1_code, actor2_code
        i.year, i.month = year, month
        i.iscri, i.iscri3, i.iscri4 = iscri, iscri3, iscri4
        i.risk = i.risk3 = i.risk4 = 0
        i.risk_date = i.iscri_date = datetime.datetime.now()
        self.context.session.add(i)
        self.context.session.commit()

    def compute(self, start_date=datetime.date(1979, 1, 1), end_date=datetime.date.today()):
        m.compute_dailies(start_date, end_date)
        m.compute_monthlies(start_date, end_date)
        m.compute_iscri_monthlies(start_date, end_date)


if __name__ == '__main__':
    art.tprint(config.name, "big")
    print("Risk Service")
    print("============")
    print(f"V{config.version}")
    print(config.copyright)
    print()
    parser = argparse.ArgumentParser(description="Event Parser")
    parser.add_argument("-e", "--echo", help="Sql Alchemy echo", action="store_true")
    args = parser.parse_args()
    context = Context()
    context.create(echo=args.echo)
    db_size = context.db_size()
    print(f"Database {context.db_name}: {db_size:.0f} Mb")
    m = RiskService(context)
    start_date = datetime.date(1979, 4, 1)
    end_date = datetime.date(2024, 10, 1)
    # end_date = datetime.date.today()
    # start_date = datetime.date(end_date.year - 1, 1, 1)
    # start_date = datetime.date(2022, 11, 1)
    # end_date = datetime.date(2022, 11, 30)


    # Month 2022-11 is not complete
    # Month 2023-03 is not complete
    # m.update_iscri(2015, 1, "USA", "CHN", 1.57)


    # m.compute_dailies(start_date, end_date)
    # m.compute_monthlies(start_date, end_date)
    m.compute_iscri_monthlies(start_date, end_date)

    print(f"Nb new daily risks: {m.nb_new_daily}")
    print(f"Nb new monthly risks: {m.nb_new_monthly}")
    print(f"Nb new iscris: {m.nb_new_iscri}")
    new_db_size = context.db_size()
    print(f"Database {context.db_name}: {new_db_size:.0f} Mb")
    print(f"Database grows: {new_db_size - db_size:.0f} Mb ({((new_db_size - db_size) / db_size) * 100:.1f}%)")
