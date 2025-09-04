import country_converter as coco
import pandas as pd
import scipy.signal
import numpy as np
import logging


class JupyterService:
    """
    Not used
    """

    def __init__(self, context):
        self.context = context
        self.df = None
        self.cc = coco.CountryConverter()
        logger = logging.getLogger("country_converter")
        logger.setLevel(logging.ERROR)

    def savgol(self, x, wl=3, p=2, mode="nearest"):
        return scipy.signal.savgol_filter(x, window_length=wl, polyorder=p, mode=mode)

    def norm_country_code(self, code: str) -> str | None:
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
        res = self.cc.convert([code], to="ISO3", enforce_list=True, not_found="None")[0][0]
        return None if res == "None" else res

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

    def get_countries(self):
        sql = "select * from country"
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

    def get_norm_iscris_by_dates_codes(self, start_year: int, start_month: int, end_year: int, end_month,
                                  actor1_code: str, actor2_code: str):
        sql = f"""SELECT iscri.*, c1.lat as lat1, c1.lon as lon1, c2.lat as lat2, c2.lon as lon2 FROM iscri
                  LEFT OUTER JOIN country c1 ON iscri.actor1_code = c1.iso3
                  LEFT OUTER JOIN country c2 ON iscri.actor2_code = c2.iso3
                  WHERE actor1_code = '{actor1_code}' AND actor2_code = '{actor2_code}'
                  AND year * 100 + month >= {start_year * 100 + start_month}
                  AND year * 100 + month <= {end_year * 100 + end_month}"""
        df = self.get_by_sql(sql + " order by iscri.year, iscri.month")
        return df


if __name__ == '__main__':
    s = JupyterService(None)
    res = s.savgol(np.arange(100))
    print(res)


