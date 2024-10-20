import datetime
from sqlalchemy import select, func
from sqlalchemy.orm import joinedload
import time
import config
from dbcontext import Context
from sqlentities import File, Event, DailyRisk, Iscri
import dateutil.relativedelta
import country_converter as coco
import pandas as pd


class JupyterService:

    def __init__(self, context):
        self.context = context
        self.df = None

    def make_sql(self, table="event", criterias: dict[str, str | float] = {}) -> str:
        sql = f"SELECT * FROM {table} "
        first = True
        for k in criterias.keys():
            if first:
                sql += "WHERE "
                first = False
            else:
                sql += " AND "
            v = criterias[k]
            if isinstance(v, str) and ("true" in v or "false" in v or "null" in v):
                sql += f"{k} is {v}"
            elif "<" in k or ">" in k:
                if isinstance(v, str):
                    sql += f"{k} '{v}'"
                else:
                    sql += f"{k} {v}"
            elif isinstance(v, str) and ("or" not in v):
                sql += f"{k} = '{v}'"
            else:
                sql += f"{k} = {v}"
        return sql

    def get_by_sql(self, sql):
        if sql.strip().lower().startswith("select"):
            print(sql)
            with self.context.engine.connect() as conn:
                self.df = pd.read_sql(sql, conn)
            return self.df
        return "Only for select statement"

    def get_file_by_date(self, date: str):
        sql = self.make_sql("file", {"id": date})
        return self.get_by_sql(sql)

    def get_files(self):
        return self.get_by_sql("select * from file order by date desc")

    def get_by_global_event_id(self, id: int):
        sql = self.make_sql(criterias={"global_event_id": id})
        self.get_by_sql(sql)
        return self.df

    def get_by_year_month_country_codes(self, year: int, month: int, actor1_country_code: str, actor2_country_code: str):
        sql = self.make_sql(criterias={"month_year": year * 100 + month, "actor1_country_code": actor1_country_code,
                                       "actor2_country_code": actor2_country_code})
        return self.get_by_sql(sql)

    def get_events_by_dates_country_codes(self, start_date: str, end_date: str,
                                         actor1_country_code: str, actor2_country_code: str):
        sql = self.make_sql(criterias={"date >=": start_date, "date <=": end_date, "is_root_event": "true",
                                       "actor1_country_code": actor1_country_code,
                                       "actor2_country_code": actor2_country_code,
                                       "(actor1_type1_code": "'GOV' OR actor2_type1_code = 'GOV')"})
        return self.get_by_sql(sql)

    def get_negative_events_by_dates_country_codes(self, start_date: str, end_date: str,
                                         actor1_country_code: str, actor2_country_code: str):
        sql = self.make_sql(criterias={"date >=": start_date, "date <=": end_date, "is_root_event": "true",
                                       "actor1_country_code": actor1_country_code,
                                       "actor2_country_code": actor2_country_code,
                                       "(actor1_type1_code": "'GOV' OR actor2_type1_code = 'GOV')",
                                       "quad_class >=": 3}
                            )
        return self.get_by_sql(sql)

    def get_risks_by_dates_codes(self, start_date: str, end_date: str, actor1_code: str, actor2_code: str):
        sql = self.make_sql("daily_risk", criterias={"date >=": start_date, "date <=": end_date,
                            "actor1_code": actor1_code, "actor2_code": actor2_code})
        return self.get_by_sql(sql)

    def get_iscris_by_year_month(self, year: int, month: int):
        sql = self.make_sql("iscri", criterias={"year": year, "month": month})
        return self.get_by_sql(sql)

    def get_iscris_by_codes(self, actor1_code: str, actor2_code: str):
        sql = self.make_sql("iscri", criterias={"actor1_code": actor1_code, "actor2_code": actor2_code})
        return self.get_by_sql(sql + " order by year, month")

    def get_iscris_by_dates_codes(self, start_year: int, start_month: int, end_year: int, end_month,
                                  actor1_code: str, actor2_code: str):
        sql = self.make_sql("iscri",
                            criterias={"actor1_code": actor1_code, "actor2_code": actor2_code,
                                       "year * 100 + month >=": start_year * 100 + start_month,
                                       "year * 100 + month <=": end_year * 100 + end_month})
        return self.get_by_sql(sql + " order by year, month")




