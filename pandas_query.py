import pandas as pd

class PandasQuery:

    def __init__(self):
        self.df = None
        pd.set_option("display.max_columns", None)
        pd.set_option("display.max_rows", None)

    def load(self, path):
        print(f"Load {path}")
        self.df = pd.read_csv(path, sep="\t", header=None, low_memory=False)
        columns = ["global_event_id", "date", "month_year", "year", "fraction_date", "actor1_code", "actor1_name",
                   "actor1_country_code", "actor1_known_group_code", "actor1_ethnic_code",
                   "actor1_religion1_code", "actor1_religion2_code", "actor1_type1_code", "actor1_type2_code",
                   "actor1_type3_code", "actor2_code", "actor2_name", "actor2_country_code", "actor2_known_group_code",
                   "actor2_ethnic_code", "actor2_religion1_code", "actor2_religion2_code",
                   "actor2_type1_code", "actor2_type2_code", "actor2_type3_code", "is_root_event", "event_code",
                   "event_base_code", "event_root_code", "quad_class", "goldstein_scale", "num_mentions", "num_sources",
                   "num_articles", "avg_tone", "actor1_geo_type", "actor1_geo_fullname", "actor1_geo_country_code",
                   "actor1_geo_adm1_code", "actor1_geo_lat", "actor1_geo_lon", "actor1_feature_id", "actor2_geo_type",
                   "actor2_geo_fullname", "actor2_geo_country_code", "actor2_geo_adm1_code", "actor2_geo_lat",
                   "actor2_geo_lon", "actor2_feature_id", "action_geo_type", "action_geo_fullname",
                   "action_geo_country_code", "action_geo_adm1_code", "action_geo_lat", "action_geo_lon",
                   "action_feature_id", "date_added"]
        if True:
            columns.append("url")
        # print(len(columns))
        self.df.columns = columns

    def filter(self, predicate):
        return self.df[predicate]

    def get_by_id(self, id):
        return self.df[self.df.global_event_id == id]



if __name__ == '__main__':
    path = "d:/iscri/download/20150201.export.CSV"
    p = PandasQuery()
    p.load(path)
    predicate = ((p.df.is_root_event == 1) &
                 ((p.df.actor1_type1_code == 'GOV') | (p.df.actor2_type1_code == 'GOV')) &
                 ((p.df.quad_class is None) | (p.df.quad_class >= 3)) &
                 # (p.df.avg_tone < 0) &
                 (p.df.goldstein_scale < 0))
    # predicate = ((p.df.avg_tone < 0))
    res = p.filter(predicate)
    print(res.avg_tone)
    print(list(res.avg_tone))
    # print(p.get_by_id(410102734).avg_tone)
