# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import time
from scrapy.exceptions import DropItem
import sqlalchemy.orm
from sc0.models import SiteMaps, Url, create_table, get_engine
import sc0.items
from scrapy import signals
import signal


class Sc0Pipeline:
    def __init__(self):
        #handle SIGTERM gracefully
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    def open_spider(self, spider):
        """
        Open spider
        
        """
        #create a logger from spiders name
        self.logger = spider.logger
        engine = get_engine(database_uri="sqlite:///" + spider.name+ ".db")
        create_table(engine)
        self.Session = sqlalchemy.orm.sessionmaker(bind=engine)
        self.item_commited= 0
        self.item_inserted = 0
        self.item_updated = 0
        self.urls_to_be_checked = []
        self.session = self.Session()
        self.commit_number = 10
        print("Sc0Pipeline initialized for " + spider.name)


    def process_item(self, item, spider):
        try:
            if type(item) == sc0.items.SiteMapItem:
                #check if record exists in SiteMaps table
                if self.session.query(SiteMaps).filter(SiteMaps.loc == item.get('loc')).first() is not None:
                    print("Sitemap already exists in database will update lastUrl")
                    self.update_db_row_bulk(item,self.session)
                else:
                    self.insert_to_db_bulk(item,self.session)
            elif type(item) == sc0.items.UrlItem:
                self.check_url_que(item,self.session)
            else:
                raise DropItem("Unknown item type")
        except Exception as e:
            raise DropItem("Error processing item: " + str(e))

        return item

    def update_db_row_bulk(self, item: sc0.items.UrlItem | sc0.items.SiteMapItem,session)-> None:
        """
        Update row in database.
        """
        try:
            #if item is a Url
            if type(item) == sc0.items.UrlItem:
                now = time.time()
                session.query(Url).filter(Url.url == item.get('url')).update(
                    {"text_content": item.get('text_content'), "scraped": item.get('scraped')})
                time_took_ms = (time.time() - now) * 1000
                print("Time took to update url: " + str(time_took_ms) + "ms")
                self.item_updated += 1
                self.commit_periodically(session)
            elif type(item) == sc0.items.SiteMapItem:
                #if item is a SiteMap
                session.query(SiteMaps).filter(SiteMaps.loc == item.get('loc')).update(
                    {"lastUrl": item.get('lastUrl') , "current_latest" : item.get('current_latest')})
                self.item_updated += 1
                self.commit_periodically(session)
            
            else:
                raise DropItem("Unknown item type")
        except:
            raise DropItem("Error updating url in database")


    def insert_to_db_bulk(self, item,session)-> None:
        """
        Insert item to database in bulk
        """
        try:
            if type(item) == sc0.items.SiteMapItem:
                sitemaps = SiteMaps()
                sitemaps.loc = item.get("loc") 
                sitemaps.lastUrl = item.get("lastUrl")
                sitemaps.current_latest = item.get("current_latest") 
                session.add(sitemaps)
                self.item_inserted += 1
                self.commit_periodically(session)
            elif type(item) == sc0.items.UrlItem :
                url = Url()
                url.text_content = item.get("text_content") 
                url.url = item.get("url") 
                url.lastmod = item.get("lastmod") 
                url.scraped = item.get("scraped") 
                session.add(url)
                self.item_inserted += 1
                self.commit_periodically(session)
            #if self.bulk_insert_item_list has more then self.commit_number items, insert them to database
            
        except Exception as e:
            raise DropItem("!Error inserting item to database: " + str(e))

    def commit_periodically(self,session):
        self.item_commited += 1
        if self.item_commited % self.commit_number == 0:
            session.commit()
            self.item_commited = 0

    def check_url_que(self,url_item,session,flush=False):
        if not flush:
            self.urls_to_be_checked.append(url_item)
        if len(self.urls_to_be_checked)%self.commit_number == 0 or flush:
            url_list = [it.get("url") for it in self.urls_to_be_checked]
            q  = session.query(Url).filter(Url.url.in_(url_list))
            records = q.all()
            urls_in_records = [it.url for it in records]
            for it in self.urls_to_be_checked:
                if it.get("url") not in urls_in_records:
                    self.insert_to_db_bulk(it,session)
                else:
                    self.update_db_row_bulk(it,session)
            self.urls_to_be_checked = []

    def close_spider(self, spider):
        """
        Close spider
        """
        if spider != None:
            self.logger.info("CLOSING THE SPIDER: " + spider.name)
        self.check_url_que(None,self.session,flush=True)
        self.session.commit()
        self.session.close()
        print("Total new items inserted to database: " + str(self.item_inserted))
        print("Total items updated in database: " + str(self.item_updated))

    def sigterm_handler(self, signum, frame):
        self.logger.info("SIGTERM received, closing the spider")
        self.close_spider(None)
        
    