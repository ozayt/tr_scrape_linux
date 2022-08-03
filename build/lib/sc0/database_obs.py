import datetime
from requests import session
import sqlalchemy.orm
from sc0.models import SiteMaps, Url, create_table, get_engine


class DatabaseObserver:
    def __init__(self,echo=False,database_uri = None) -> None:
        """
        Create database tables and establish Session class from sessionmaker factory.
        #TODO take a look at how sessions works
        """
        engine = get_engine(echo=echo , database_uri = database_uri)
        create_table(engine)
        self.Session = sqlalchemy.orm.sessionmaker(bind=engine)
        print("DatabaseManager initialized")
    
    def url_exists(self, url: str) -> bool:
        """
        Check if url exists in database.
        """
        session = self.Session()
        try:
            if session.query(Url).filter(Url.url == url).first() is None:
                return False
            else:
                return True
        except Exception as e:
            print("Something went wrong : " + str(e))
            raise e
        finally:
            session.close()

    def sitemap_exists(self, sitemap_url: str) -> bool:
        """
        Check if sitemap exists in database.
        #TODO Checking one by one is fine ?
        """
        session = self.Session()
        try:
            if session.query(SiteMaps).filter(SiteMaps.loc == sitemap_url).first() is None:
                return False
            else:
                return True
        except Exception as e:
            print("Something went wrong : " + str(e))
            raise e
        finally:
            session.close()
    
    #get all urls from database where lastmod is newer than date given
    def get_newer_urls(self, date: datetime.datetime) -> list:
        """Get all urls from database where lastmod is newer than date given.

        Args:
            date (datetime.datetime): Date to compare lastmod to.

        Raises:
            e: _description_

        Returns:
            list: List of urls that are newer than date.
        """
        session = self.Session()
        try:
            urls = session.query(Url).filter(Url.lastmod > date).all()
            return urls
        except Exception as e:
            print("Something went wrong : " + str(e))
            raise e
        finally:
            session.close()
    
    def get_unscraped_urls_as_str(self) -> list[str]:
        """Get all urls from database where scraped is False.

        Raises:
            e: _description_

        Returns:
            list: List of urls that are unscraped.
        """
        session = self.Session()
        try:
            # get all urls from database where scraped is False as a list of strings
            urls = [obj[0] for obj in session.query(Url.url).filter(Url.scraped == False).all()]
            return urls
        except Exception as e:
            print("Couldnt get unscraped URLs from database : " + str(e))
            raise e
        finally:
            session.close()

    def get_all_sitemaps(self) -> list[SiteMaps]:
        """Get all sitemaps from database.

        Raises:
            e: _description_
        """
        session = self.Session()
        try:
            sitemaps = session.query(SiteMaps).all()
            return sitemaps
        except Exception as e:
            print("Something went wrong : " + str(e))
            raise e
        finally:
            session.close()
    
    def get_sitemap(self, sitemap_url: str) -> SiteMaps|None:
        """Get sitemap from database.

        Args:
            sitemap_url (str): Url of sitemap.

        Raises:
            e: _description_
        """
        session = self.Session()
        try:
            sitemap = session.query(SiteMaps).filter(SiteMaps.loc == sitemap_url).first()
            return sitemap
        except Exception as e:
            print("Something went wrong : " + str(e))
            raise e
        finally:
            session.close()
        