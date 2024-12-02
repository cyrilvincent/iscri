from sqlalchemy import Column, ForeignKey, Boolean, UniqueConstraint, Index, DateTime
from sqlalchemy.types import BigInteger, Integer, String, Float, Date, SmallInteger
from sqlalchemy.orm import relationship, Mapped
from dbcontext import Base

# file 1-* event -1 url


class File(Base):
    __tablename__ = "file"

    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False)
    date = Column(Date, unique=True, nullable=False)
    online_date = Column(DateTime)
    download_date = Column(DateTime)
    md5 = Column(String(50))
    dezip_date = Column(DateTime)
    import_start_date = Column(DateTime)
    import_end_date = Column(DateTime)
    events: Mapped[list["Event"]] = relationship(back_populates="file")

    def __repr__(self):
        return f"{self.id} {self.name}"


class Event(Base):
    __tablename__ = "event"

    id = Column(BigInteger, primary_key=True)
    global_event_id = Column(BigInteger, nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    # day = Column(BigInteger, nullable=False)
    month_year = Column(Integer, nullable=False, index=True)
    year = Column(SmallInteger, nullable=False)
    # fraction_date = Column(Float, nullable=False)
    gdelt_date = Column(Date, nullable=False)
    file_id = Column(ForeignKey('file.id'), nullable=False, index=True)
    file: Mapped[File] = relationship(back_populates="events")
    # event_actors: Mapped[list["EventActor"]] = relationship(back_populates="event")
    # event_geos: Mapped[list["EventGeo"]] = relationship(back_populates="event")
    # actor1_is_gov = Column(Boolean, nullable=False)
    actor1_code = Column(String(50))
    actor1_name = Column(String(255))
    actor1_country_code = Column(String(3))
    actor1_known_group_code = Column(String(3))
    actor1_ethnic_code = Column(String(3))
    actor1_religion1_code = Column(String(3))
    actor1_religion2_code = Column(String(3))
    actor1_type1_code = Column(String(3))
    actor1_type2_code = Column(String(3))
    actor1_type3_code = Column(String(3))
    # actor2_is_gov = Column(Boolean, nullable=False)
    actor2_code = Column(String(50))
    actor2_name = Column(String(255))
    actor2_country_code = Column(String(3))
    actor2_known_group_code = Column(String(3))
    actor2_ethnic_code = Column(String(3))
    actor2_religion1_code = Column(String(3))
    actor2_religion2_code = Column(String(3))
    actor2_type1_code = Column(String(3))
    actor2_type2_code = Column(String(3))
    actor2_type3_code = Column(String(3))
    is_root_event = Column(Boolean, nullable=False)
    event_code = Column(String(10))
    event_base_code = Column(String(10))
    event_root_code = Column(String(10))
    quad_class = Column(SmallInteger, nullable=False)
    goldstein_scale = Column(Float)
    num_mentions = Column(Integer, nullable=False)
    num_sources = Column(Integer, nullable=False)
    num_articles = Column(Integer, nullable=False)
    avg_tone = Column(Float, nullable=False)
    actor1_geo_type = Column(Integer)
    actor1_geo_fullname = Column(String(255))
    actor1_geo_country_code = Column(String(3))
    actor1_geo_adm1_code = Column(String(10))
    actor1_geo_lat = Column(Float)
    actor1_geo_lon = Column(Float)
    actor1_feature_id = Column(String(10))
    actor2_geo_type = Column(Integer)
    actor2_geo_fullname = Column(String(255))
    actor2_geo_country_code = Column(String(3))
    actor2_geo_adm1_code = Column(String(10))
    actor2_geo_lat = Column(Float)
    actor2_geo_lon = Column(Float)
    actor2_feature_id = Column(String(10))
    action_geo_type = Column(Integer)
    action_geo_fullname = Column(String(255))
    action_geo_country_code = Column(String(3))
    action_geo_adm1_code = Column(String(10))
    action_geo_lat = Column(Float)
    action_geo_lon = Column(Float)
    action_feature_id = Column(String(10))
    date_added = Column(Date)
    url_id = Column(ForeignKey('url.id'))
    url: Mapped["Url"] = relationship()
    parse_date = Column(DateTime, nullable=False)

    __table_args__ = (Index('ix_event_date_actor1_type1_code_actor_type1_code_is_root_event', 'date', 'actor1_type1_code', 'actor2_type1_code', 'is_root_event'), )

    def __repr__(self):
        return f"{self.id} {self.global_event_id}"


class Url(Base):
    __tablename__ = "url"

    id = Column(BigInteger, primary_key=True)
    url = Column(String(1024), nullable=False)

    def __repr__(self):
        return f"{self.id} {self.url}"


class DailyRisk(Base):
    __tablename__ = "daily_risk"

    id = Column(BigInteger, primary_key=True)
    date = Column(Date, nullable=False)
    actor1_code = Column(String(50), nullable=False)
    actor2_code = Column(String(50), nullable=False)
    quad3_nb = Column(Integer, nullable=False)
    quad4_nb = Column(Integer, nullable=False)
    total_nb = Column(Integer, nullable=False)
    goldstein_nb = Column(Integer, nullable=False)
    goldstein_quad3_nb = Column(Integer, nullable=False)
    goldstein_quad4_nb = Column(Integer, nullable=False)
    goldstein3_sum = Column(Float, nullable=False)
    goldstein4_sum = Column(Float, nullable=False)
    compute_date = Column(DateTime, nullable=False)

    __table_args__ = (UniqueConstraint('date', 'actor1_code', 'actor2_code'),)

    @property
    def key(self):
        return self.date, self.actor1_code, self.actor2_code

    @property
    def risk(self):
        return (self.risk3_nb + self.risk4_nb) / self.total_nb

    def __repr__(self):
        return f"{self.id} {self.date} {self.actor1_code} {self.actor2_code} {self.risk}"


class Iscri(Base):
    __tablename__ = "iscri" # monthly

    id = Column(BigInteger, primary_key=True)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    actor1_code = Column(String(50), nullable=False)
    actor2_code = Column(String(50), nullable=False)
    risk = Column(Float, nullable=False)
    risk3 = Column(Float, nullable=False)
    risk4 = Column(Float, nullable=False)
    # risk_g3 = Column(Float, nullable=False)
    # risk_g4 = Column(Float, nullable=False)
    # risk_g34 = Column(Float, nullable=False)
    risk3g = Column(Float, nullable=False)
    risk4g = Column(Float, nullable=False)
    riskg = Column(Float, nullable=False)
    # risk_g = Column(Float, nullable=False)
    risk_date = Column(DateTime, nullable=False)
    iscri = Column(Float)
    iscri3 = Column(Float)
    iscri4 = Column(Float)
    iscrig = Column(Float)
    iscri3g = Column(Float)
    iscri4g = Column(Float)
    # iscri_g3 = Column(Float)
    # iscri_g4 = Column(Float)
    # iscri_g34 = Column(Float)
    # iscri_g = Column(Float)
    iscri_date = Column(DateTime)

    __table_args__ = (UniqueConstraint('year', 'month', 'actor1_code', 'actor2_code'),
                      Index('ix_iscri_year_month', 'year', 'month'))

    @property
    def key(self):
        return self.year, self.month, self.actor1_code, self.actor2_code

    def __repr__(self):
        return f"{self.id} {self.year}{self.month:02d} {self.actor1_code} {self.actor2_code}"

