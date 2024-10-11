from sqlalchemy import Column, ForeignKey, Boolean, UniqueConstraint, Table, Index, DateTime
from sqlalchemy.types import BigInteger, Integer, String, Float, Date, SmallInteger
from sqlalchemy.orm import relationship, Mapped, mapped_column
from dbcontext import Base

# file 1-* event  -1 url
#                1-* event_actor *-1 actor
#                1-* event_geo *-1 geo


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
    date = Column(Date, nullable=False)
    # day = Column(BigInteger, nullable=False)
    month_year = Column(Integer, nullable=False, index=True)
    year = Column(SmallInteger, nullable=False)
    # fraction_date = Column(Float, nullable=False)
    gdlet_date = Column(Date, nullable=False)
    file_id = Column(ForeignKey('file.id'), nullable=False)
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
    event_code = Column(String(10), nullable=False)
    event_base_code = Column(String(10), nullable=False)
    event_root_code = Column(String(10), nullable=False)
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
    date_added = Column(Date, nullable=False)
    url_id = Column(ForeignKey('url.id'))
    url: Mapped["Url"] = relationship()
    parse_date = Column(DateTime, nullable=False)

    __table_args__ = (Index('ix_event_date_actor1_type1_code_actor_type1_code_is_root_event', 'date', 'actor1_type1_code', 'actor2_type1_code', 'is_root_event'), )

    def __repr__(self):
        return f"{self.id} {self.global_event_id}"


# class Actor(Base):
#     __tablename__ = "actor"
#
#     id = Column(Integer, primary_key=True)
#     code = Column(String(50), index=True)
#     event_actors: Mapped[list["EventActor"]] = relationship(back_populates="actor")
#     name = Column(String(255))
#     country_code = Column(String(3), index=True)
#     known_group_code = Column(String(3))
#     ethnic_code = Column(String(3))
#     religion1_code = Column(String(3))
#     religion2_code = Column(String(3))
#     type1_code = Column(String(3))
#     type2_code = Column(String(3))
#     type3_code = Column(String(3))
#     parse_date = Column(DateTime, nullable=False)
#
#     @property
#     def key(self):
#         return (self.code, self.name, self.country_code, self.known_group_code, self.ethnic_code, self.religion1_code,
#                 self.religion2_code, self.type1_code, self.type2_code, self.type3_code)
#
#     def __repr__(self):
#         return f"{self.id} {self.code} {self.name}"
#
#
# class EventActor(Base):
#     __tablename__ = "event_actor"
#
#     id = Column(BigInteger, primary_key=True)
#     event_id = Column(ForeignKey('event.id'), nullable=False)
#     event: Mapped[Event] = relationship(back_populates="event_actors")
#     actor_id = Column(ForeignKey('actor.id'), nullable=False)
#     actor: Mapped[Actor] = relationship(back_populates="event_actors")
#     num = Column(SmallInteger, nullable=False)
#
#     __table_args__ = (UniqueConstraint('event_id', 'actor_id', 'num'),)
#
#     @property
#     def key(self):
#         return f"{self.event_id} {self.actor_id} {self.num}"
#
#     def __repr__(self):
#         return f"{self.id} {self.event_id} {self.actor_id} {self.num}"
#
#
# class Geo(Base):
#     __tablename__ = "geo"
#
#     id = Column(Integer, primary_key=True)
#     type = Column(SmallInteger, nullable=False)
#     fullname = Column(String(255), index=True)
#     country_code = Column(String(3), index=True)
#     adm1_code = Column(String(10))
#     lat = Column(Float)
#     lon = Column(Float)
#     feature_id = Column(String(10), index=True)
#     event_geos: Mapped[list["EventGeo"]] = relationship(back_populates="geo")
#     parse_date = Column(DateTime, nullable=False)
#
#     @property
#     def key(self):
#         return self.type, self.fullname if self.feature_id is None else self.feature_id # D'après la doc GDELT-Data_Format.pdf chapitre event geography
#
#     def __repr__(self):
#         return f"{self.id} {self.type} {self.fullname} {self.feature_id}"
#
#
# class EventGeo(Base):
#     __tablename__ = "event_geo"
#
#     id = Column(BigInteger, primary_key=True)
#     event_id = Column(ForeignKey('event.id'), nullable=False)
#     event: Mapped[Event] = relationship(back_populates="event_geos")
#     geo_id = Column(ForeignKey('geo.id'), nullable=False)
#     geo: Mapped[Geo] = relationship(back_populates="event_geos")
#     num = Column(SmallInteger, nullable=False) # 1,2,3=action
#
#     __table_args__ = (UniqueConstraint('event_id', 'geo_id', 'num'), )
#
#     @property
#     def key(self):
#         return f"{self.event_id} {self.geo_id} {self.num}"
#
#     def __repr__(self):
#         return f"{self.id} {self.event_id} {self.geo_id} {self.num}"
#


class Url(Base):
    __tablename__ = "url"

    id = Column(BigInteger, primary_key=True)
    url = Column(String(1024), nullable=False)

    def __repr__(self):
        return f"{self.id} {self.url}"


class DailyRisk(Base):
    __tablename__ = "daily_risk"

    id = Column(BigInteger, primary_key=True)
    date = Column(Date, nullable=False, unique=True)
    actor1_code = Column(String(50), nullable=False)
    actor2_code = Column(String(50), nullable=False)
    quad3_nb = Column(Float, nullable=False)
    quad4_nb = Column(Float, nullable=False)
    total_nb = Column(Float, nullable=False)
    article3_nb = Column(Float, nullable=False)
    article4_nb = Column(Float, nullable=False)
    total_article_nb = Column(Float, nullable=False)
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
    quad3_nb = Column(Float, nullable=False)
    quad4_nb = Column(Float, nullable=False)
    total_nb = Column(Float, nullable=False)
    article3_nb = Column(Float, nullable=False)
    article4_nb = Column(Float, nullable=False)
    total_article_nb = Column(Float, nullable=False)
    compute_date = Column(DateTime, nullable=False)
    iscri_score = Column(Float)
    iscri_date = Column(DateTime)

    __table_args__ = (UniqueConstraint('year', 'month', 'actor1_code', 'actor2_code'),
                      Index('ix_iscri_year_month', 'year', 'month'))

    @property
    def key(self):
        return self.year, self.month, self.actor1_code, self.actor2_code

    def __repr__(self):
        return f"{self.id} {self.year}{self.month} {self.actor1_code} {self.actor2_code}"

