from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Boolean, create_engine , Text
from scrapy.utils.project import get_project_settings
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()

def get_engine(echo=False,database_uri = None):
    """Create a database engine from scrapy settings.py "SQLALCHEMY_DATABASE_URI"
    Returns:
        sqlalchemy.engine.base.Engine: A database engine
    """
    if database_uri is None:
        database_uri = get_project_settings().get("SQLALCHEMY_DATABASE_URI")
    return create_engine(database_uri,echo=echo)  # type: ignore

def create_table(engine):
    Base.metadata.create_all(engine)


class Url(Base):
    __tablename__ = "urls" 
    id = Column(Integer, primary_key=True)
    text_content = Column(Text)
    url = Column(String(2048), index=True)
    lastmod = Column(DateTime)
    scraped = Column(Boolean)
    
class SiteMaps(Base):
    __tablename__ = "sitemaps" 
    id = Column(Integer, primary_key=True)
    loc = Column(String)
    lastUrl = Column(String)
    current_latest = Column(Boolean)