import argparse
import datetime
import art
from sqlalchemy import select
import calendar
import config
from dbcontext import Context
from sqlentities import File, Event, DailyRisk, Iscri
import dateutil.relativedelta


class RiskService:
    """
    Risk compute
    """

    def __init__(self, context):
        """
        :param context: SqlAlchemy context
        daily_set : list of days already computed in cache set
        monthly_set : list of tuple year month already computed in cache set
        nb_ram : not used
        nb_new_daily : nb new daily risks computed and commited
        nb_new_monthly : nb new monthly risks computed and commited
        iscri_accelaration : iscri a parameter in ISCRIm = RISKm + a * ISCRIm-1
        """
        self.context = context
        self.daily_set: set[datetime.date] = set()
        self.monthly_set: set[tuple[int, int]] = set()
        self.nb_ram = 0  # to be removed
        self.nb_new_daily = 0
        self.nb_new_monthly = 0
        self.nb_new_iscri = 0
        self.iscri_acceleration = 0.9

    def last_day_of_month(self, year: int, month: int) -> int:
        """
        :param year:
        :param month:
        :return: last day of the month
        """
        return calendar.monthrange(year, month)[1]

    def month_range(self, start_date: datetime.date, end_date: datetime.date):
        """
        Month generator
        :param start_date:
        :param end_date:
        :return: generator
        """
        end = datetime.date(end_date.year, end_date.month, self.last_day_of_month(end_date.year, end_date.month))
        date = start_date
        while date < end:
            date = datetime.date(date.year, date.month, self.last_day_of_month(date.year, date.month))
            yield date
            date = date + dateutil.relativedelta.relativedelta(months=1)

    def date_range(self, start_date: datetime.date, end_date: datetime.date):
        """
        Date generator
        :param start_date:
        :param end_date:
        :return: the generator
        """
        days = int((end_date - start_date).days)
        for n in range(days):
            yield start_date + datetime.timedelta(n)

    def is_all_files_presents_by_year_month(self, year: int, month: int) -> bool:
        """
        Check if a month have all files
        :param year:
        :param month:
        :return: True if month is complete
        """
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
        # Quering all file in the month
        l = (self.context.session.execute(
                select(File.id)
                .where((File.date >= datetime.date(year, month, 1)) &
                       (File.date <= datetime.date(year, month, self.last_day_of_month(year, month))) &
                       (File.import_end_date.isnot(None))))
             .scalars().all())
        # Hard coded
        # File not present on gdelt
        nb_not_in_html = 0
        if year == 2014 and month == 1:
            nb_not_in_html = 3
        elif year == 2014 and month == 3:
            nb_not_in_html = 1
        elif year == 2022 and month == 11:
            nb_not_in_html = 1
        elif year == 2023 and month == 3:
            nb_not_in_html = 2
        elif year == 2025 and month == 6:
            nb_not_in_html = 17
        elif year == 2025 and month == 7:
            nb_not_in_html = 1
        # Check is all file present
        res = len(l) >= self.last_day_of_month(year, month) - nb_not_in_html
        return res

    def compute_daily(self, d: datetime.date) -> dict[tuple[str, str], DailyRisk] | None:
        """
        Compute all DailyRisk for a day
        :param d: the day
        :return: A dict, key = pair of actor, value = the DailyRisk
        """
        daily_dict: dict[tuple[str, str], DailyRisk] = {}
        l: list[Event] = self.context.session.execute(
            select(Event).
            where((Event.date == d) & Event.is_root_event &
                  ((Event.actor1_type1_code == "GOV") | (Event.actor2_type1_code == "GOV")) &
                  (Event.actor1_country_code.isnot(None)) & (Event.actor2_country_code.isnot(None))
                  )).scalars().all()
        first = True
        for e in l:
            key: tuple[str, str] = e.actor1_country_code, e.actor2_country_code
            if key not in daily_dict:
                if first:
                    print(f"Compute daily risk {d}")
                    first = False
                dr = DailyRisk()
                dr.total_nb = dr.quad3_nb = dr.quad4_nb = 0
                dr.goldstein_quad3_nb = dr.goldstein_quad4_nb = dr.goldstein_nb = 0
                dr.goldstein3_sum = dr.goldstein4_sum = 0
                dr.actor1_code, dr.actor2_code = key
                dr.date = d
                dr.compute_date = datetime.datetime.now()
                daily_dict[key] = dr
            daily_dict[key].total_nb += 1
            if e.quad_class == 3:
                daily_dict[key].quad3_nb += 1
                if e.goldstein_scale is not None and e.goldstein_scale < 0:
                    daily_dict[key].goldstein3_sum += e.goldstein_scale
            elif e.quad_class == 4:
                daily_dict[key].quad4_nb += 1
                if e.goldstein_scale is not None and e.goldstein_scale < 0:
                    daily_dict[key].goldstein4_sum += e.goldstein_scale
            if e.goldstein_scale is not None and e.goldstein_scale < 0:
                daily_dict[key].goldstein_nb += 1
                if e.quad_class == 3:
                    daily_dict[key].goldstein_quad3_nb += 1
                elif e.quad_class == 4:
                    daily_dict[key].goldstein_quad4_nb += 1
        return daily_dict

    def compute_dailies(self, start_date=datetime.date(1979, 1, 1), end_date=datetime.date.today()):
        """
        Compute all DailyRisks for a range of day
        :param start_date:
        :param end_date:
        """
        print("Compute daily risks")
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
        """
        Compute all Risks for a month
        :param last_month_day: the last day of the month
        :return: A dict with key = pair of actor, value = Iscri with only risk computed, not iscris
        """
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
                i.risk3g = i.risk4g = i.riskg = 0
                dico[e.actor1_code, e.actor2_code] = i
            i = dico[(e.actor1_code, e.actor2_code)]
            if e.total_nb != 0:
                i.risk += ((e.quad3_nb + e.quad4_nb) / e.total_nb) / last_month_day.day
                i.risk3 += (e.quad3_nb / e.total_nb) / last_month_day.day  # * ( 1 + coef) * (-e.goldstein3.sum / e.quad3_nb) if quad3_nb != 0
                i.risk4 += (e.quad4_nb / e.total_nb) / last_month_day.day
                if e.quad3_nb != 0:
                    i.risk3g += (((e.quad3_nb / e.total_nb) / last_month_day.day) * -e.goldstein3_sum / e.quad3_nb)
                if e.quad4_nb != 0:
                    i.risk4g += (((e.quad4_nb / e.total_nb) / last_month_day.day) * -e.goldstein4_sum / e.quad4_nb)
                if e.quad3_nb + e.quad4_nb != 0:
                    i.riskg += (((e.quad3_nb + e.quad4_nb) / e.total_nb) / last_month_day.day * (-e.goldstein4_sum - e.goldstein3_sum) / (e.quad4_nb + e.quad3_nb))
        return dico

    def compute_monthlies(self, start_date=datetime.date(1979, 1, 1), end_date=datetime.date.today()):
        """
        Compute all Risks for a range of month
        :param start_date:
        :param end_date:
        """
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
                if self.is_all_files_presents_by_year_month(m.year, m.month):
                    dico = self.compute_monthly(m)
                    for i in dico.values():
                        if i.risk != 0:
                            self.context.session.add(i)
                            self.nb_new_monthly += 1
                    self.context.session.commit()
                else:
                    print(f"Month {m.year}-{m.month:02d} is not complete")

    def iscri(self, risk: float, previous: float) -> float:
        """
        Compute an ISCRI
        ISRCIm = RISKm + 0.9 * ISCRIm-1
        :param risk: the risk
        :param previous: the prévious month iscri
        :return: ISCRIm
        """
        return risk + self.iscri_acceleration * previous

    def compute_iscri(self, i: Iscri, previous: Iscri | None) -> Iscri:
        """
        Compute an ISCRI SqlAlchemy entity
        :param i: the ISCRI
        :param previous: ISCRIm-1
        :return: the ISCRI with computed ISCRI
        """
        if previous is None:
            previous = Iscri()
            previous.iscri3 = previous.iscri4 = previous.iscri = 0
            previous.iscrig = previous.iscri3g = previous.iscri4g = 0
        i.iscri3 = self.iscri(i.risk3, previous.iscri3)
        i.iscri4 = self.iscri(i.risk4, previous.iscri4)
        i.iscri = self.iscri(i.risk, previous.iscri)
        i.iscri3g = self.iscri(i.risk3g, previous.iscri3g)
        i.iscri4g = self.iscri(i.risk4g, previous.iscri4g)
        i.iscrig = self.iscri(i.riskg, previous.iscrig)
        i.iscri_date = datetime.datetime.now()
        return i

    def compute_iscri_monthly(self, year: int, month: int):
        """
        Compute all ISCRIs for a month and saved
        :param year:
        :param month:
        """
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
                                (Iscri.iscri > config.iscri_threshold))).scalars().all()
        for e in l:
            if (e.actor1_code, e.actor2_code) not in dico:
                i = Iscri()
                i.risk3 = i.risk4 = i.risk = 0  # For iscri when no risk in the current month
                i.risk3g = i.risk4g = i.riskg = 0
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
        """
        Compute all ISCRIs for a range of month and saved
        :param start_date:
        :param end_date:
        """
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
        """
        Not used
        :param year:
        :param month:
        :param actor1_code:
        :param actor2_code:
        :param iscri:
        :param iscri3:
        :param iscri4:
        :return:
        """
        i = (self.context.session.execute(
            select(Iscri).where((Iscri.year == year) & (Iscri.month == month) &
                                (Iscri.actor1_code == actor1_code) & (Iscri.actor2_code == actor2_code))).
             scalars().first())
        i.iscri, i.iscri3, i.iscri4 = iscri, iscri3, iscri4
        i.iscri_date = datetime.datetime.now()
        self.context.session.commit()

    def create_iscri(self, year: int, month: int, actor1_code: str, actor2_code: str,
                     iscri: float, iscri3=0.0, iscri4=0.0):
        """
        Create a ISCRI entity for a new pair of actor and a new month
        :param year:
        :param month:
        :param actor1_code: actor1 code
        :param actor2_code: actor2 code
        :param iscri: the ISCRI value
        :param iscri3: the ISCRI3 value
        :param iscri4: the ISCRI4 value
        """
        i = Iscri()
        i.actor1_code, i.actor2_code = actor1_code, actor2_code
        i.year, i.month = year, month
        i.iscri, i.iscri3, i.iscri4 = iscri, iscri3, iscri4
        i.risk = i.risk3 = i.risk4 = 0
        i.risk_date = i.iscri_date = datetime.datetime.now()
        self.context.session.add(i)
        self.context.session.commit()


if __name__ == '__main__':
    art.tprint(config.name, "big")
    print("Risk Service")
    print("============")
    print(f"V{config.version}")
    print(config.copyright)
    print()
    parser = argparse.ArgumentParser(description="Event Parser")
    parser.add_argument("-e", "--echo", help="Sql Alchemy echo", action="store_true")
    parser.add_argument("-d", "--daily", help="Compute risks daily", action="store_true")
    parser.add_argument("-m", "--monthly", help="Compute risks monthly", action="store_true")
    parser.add_argument("-i", "--iscri", help="Compute iscris monthly", action="store_true")
    args = parser.parse_args()
    context = Context()
    context.create(echo=args.echo)
    db_size = context.db_size()
    print(f"Database {context.db_name}: {db_size:.0f} Mb")
    m = RiskService(context)
    # start_date = datetime.date(1979, 4, 1)
    # end_date = datetime.date(2024, 10, 1)
    end_date = datetime.date.today()
    start_date = datetime.date(end_date.year - 1, 1, 1)

    if args.daily:
        m.compute_dailies(start_date, end_date)
    if args.monthly:
        m.compute_monthlies(start_date, end_date)
    if args.iscri:
        m.compute_iscri_monthlies(start_date, end_date)

    print(f"Nb new daily risks: {m.nb_new_daily}")
    print(f"Nb new monthly risks: {m.nb_new_monthly}")
    print(f"Nb new iscris: {m.nb_new_iscri}")
    new_db_size = context.db_size()
    print(f"Database {context.db_name}: {new_db_size:.0f} Mb")
    print(f"Database grows: {new_db_size - db_size:.0f} Mb ({((new_db_size - db_size) / db_size) * 100:.1f}%)")
