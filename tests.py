import datetime
from unittest import TestCase

from sqlalchemy import select
from sqlalchemy.orm import joinedload

from event_parser import EventParser
from risk_service import RiskService
from sqlentities import *
from dbcontext import *


class GDLETTests(TestCase):

    def test_test_file(self):
        p = EventParser(None)
        p.test_file("data/20240921.small.CSV")

    def test_mapper(self):
        p = EventParser(None)
        s = "1199516024	20140924	201409	2014	2014.7233	GOV	PRESIDENT						GOV			MIL	MILITARY						MIL			1	040	040	04	1	1.0	3	1	3	-0.76045627376426	1	Spain	SP	SP	40	-4	SP	1	Spain	SP	SP	40	-4	SP	1	Spain	SP	SP	40	-4	SP	20240921	https://www.saltlakecitysun.com/news/274614956/middle-east-media-research-institute-memri-publishes-a-collection-of-speeches-by-turkish-president-recep-tayyip-erdo287an-from-2005-to-2024"
        row = s.split("\t")
        e = p.mapper(row)
        self.assertEqual(1199516024, e.id)
        self.assertEqual(datetime.date(2014,9,24), e.date)
        self.assertEqual(20140924, e.day)
        self.assertEqual(201409, e.month_year)
        self.assertEqual(2014, e.year)
        self.assertEqual(2014.7233, e.fraction_date)
        self.assertEqual("GOV", e.actor1_code)
        self.assertEqual("PRESIDENT", e.actor1_name)
        self.assertEqual("GOV", e.actor1_type1_code)
        self.assertEqual("MIL", e.actor2_code)
        self.assertEqual("MILITARY", e.actor2_name)
        self.assertEqual("MIL", e.actor2_type1_code)
        self.assertEqual(1, e.is_root_event)
        self.assertEqual("040", e.event_code)
        self.assertEqual("040", e.event_base_code)
        self.assertEqual("04", e.event_root_code)
        self.assertEqual(1, e.quad_class)
        self.assertEqual(1.0, e.goldstein_scale)
        self.assertEqual(3, e.num_mentions)
        self.assertEqual(1, e.num_sources)
        self.assertEqual(3, e.num_articles)
        self.assertAlmostEqual(-0.76, e.avg_tone, delta=1e-2)
        # 1	Spain	SP	SP	40	-4	SP	1	Spain	SP	SP	40	-4	SP	1	Spain	SP	SP	40	-4	SP
        self.assertEqual(1, e.actor1_geo_type)
        self.assertEqual("Spain", e.actor1_geo_fullname)
        self.assertEqual("SP", e.actor1_geo_country_code)
        self.assertEqual("SP", e.actor1_geo_adm1_code)
        self.assertEqual(40, e.actor1_geo_lat)
        self.assertEqual(-4, e.actor1_geo_lon)
        self.assertEqual("SP", e.actor1_feature_id)
        self.assertEqual(1, e.actor2_geo_type)
        self.assertEqual("Spain", e.actor2_geo_fullname)
        self.assertEqual("SP", e.actor2_geo_country_code)
        self.assertEqual("SP", e.actor2_geo_adm1_code)
        self.assertEqual(40, e.actor2_geo_lat)
        self.assertEqual(-4, e.actor2_geo_lon)
        self.assertEqual("SP", e.actor2_feature_id)
        self.assertEqual(20240921, e.date_added)

    def test_actor_mapper(self):
        p = EventParser(None)
        s = "1199516024	20140924	201409	2014	2014.7233	GOV	PRESIDENT						GOV			MIL	MILITARY						MIL			1	040	040	04	1	1.0	3	1	3	-0.76045627376426	1	Spain	SP	SP	40	-4	SP	1	Spain	SP	SP	40	-4	SP	1	Spain	SP	SP	40	-4	SP	20240921	https://www.saltlakecitysun.com/news/274614956/middle-east-media-research-institute-memri-publishes-a-collection-of-speeches-by-turkish-president-recep-tayyip-erdo287an-from-2005-to-2024"
        row = s.split("\t")
        e = p.actor_mapper(row, 1)
        self.assertEqual("GOV", e.code)
        self.assertEqual("PRESIDENT", e.name)
        self.assertEqual("GOV", e.type1_code)
        e = p.actor_mapper(row, 2)
        self.assertEqual("MIL", e.code)
        self.assertEqual("MILITARY", e.name)
        self.assertEqual("MIL", e.type1_code)
        self.assertIsNotNone(e.parse_date)

    def test_url_mapper(self):
        p = EventParser(None)
        s = "1199516024	20140924	201409	2014	2014.7233	GOV	PRESIDENT						GOV			MIL	MILITARY						MIL			1	040	040	04	1	1.0	3	1	3	-0.76045627376426	1	Spain	SP	SP	40	-4	SP	1	Spain	SP	SP	40	-4	SP	1	Spain	SP	SP	40	-4	SP	20240921	https://www.saltlakecitysun.com/news/274614956/middle-east-media-research-institute-memri-publishes-a-collection-of-speeches-by-turkish-president-recep-tayyip-erdo287an-from-2005-to-2024"
        row = s.split("\t")
        e = p.url_mapper(row)
        self.assertEqual("https://www.saltlakecitysun.com/news/274614956/middle-east-media-research-institute-memri-publishes-a-collection-of-speeches-by-turkish-president-recep-tayyip-erdo287an-from-2005-to-2024", e.url)

    def test_select_file(self):
        context = Context()
        context.create(echo=True)
        res = context.session.execute(
            select(File).options(joinedload(File.events)).outerjoin(Event)
            .where(File.name == "0.test.csv")).scalars().first()
        self.assertIsNotNone(res)

    def test_parse_row(self):
        context = Context()
        context.create(echo=True)
        p = EventParser(context)
        s = "1199516024	20140924	201409	2014	2014.7233	GOV	PRESIDENT						GOV			MIL	MILITARY						MIL			1	040	040	04	1	1.0	3	1	3	-0.76045627376426	1	Spain	SP	SP	40	-4	SP	1	Spain	SP	SP	40	-4	SP	1	Spain	SP	SP	40	-4	SP	20240921	https://www.saltlakecitysun.com/news/274614956/middle-east-media-research-institute-memri-publishes-a-collection-of-speeches-by-turkish-president-recep-tayyip-erdo287an-from-2005-to-2024"
        row = s.split("\t")
        p.path = "0.test.csv"
        p.get_file()
        p.load_cache()
        p.parse_row(row)

    def test_date_iterator(self):
        s = RiskService(None)
        l = list(s.date_iterator())
        self.assertTrue(len(l) > 0)

