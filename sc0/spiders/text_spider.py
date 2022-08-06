from abc import abstractmethod
import logging
from requests import request
import scrapy
import bs4
logging.getLogger('trafilatura').setLevel(logging.WARNING)
import trafilatura
import datetime
import sc0.database_obs
import sc0.items
from sc0.models import SiteMaps , Url
import signal
import scrapy.signals
from scrapy.signalmanager import SignalManager
class CustomSitemapSpider(scrapy.Spider):

    def __init__(self, db:sc0.database_obs.DatabaseObserver,*args, **kwargs):
        self.db = db
        self.scrape_unscraped_db  = False
        super().__init__(*args, **kwargs)
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    def reqs_items_from_sitemapindex(self , newest_sitemap_loc:str , 
    other_sitemap_locs_newest_to_oldest:list[str])->tuple[list[scrapy.Request],list[sc0.items.SiteMapItem]]:
        """ This function will return a list of requests to make by looking at the database for the newest sitemap and the other sitemaps.
        Args:
            newest_sitemap_loc (str): The url of the newest sitemap
            other_sitemap_locs_newest_to_oldest (list[str]): The urls of the other sitemaps, newest to oldest.
        Returns:
            tuple[list[scrapy.Request],list[sc0.items.SiteMapItem]]: A tuple of two lists. The first list is the requests to make, the second list is the sitemaps items to update.
        """
        requests = []
        items =[]
        sitemaps = self.db.get_all_sitemaps()
        sitemap_dict:dict= {sitemap.loc:sitemap for sitemap in sitemaps}
        for loc in other_sitemap_locs_newest_to_oldest:
            if self.db.get_sitemap(loc) == None:
                requests.append(scrapy.Request(loc, callback=self.parse_period, meta={"current_latest":False}))
            else:
                self.logger.info("OLD SITEMAP ALREADY IN DB: %s ",loc)
                #check database if sitemap has newest flag set to true
                if sitemap_dict[loc].current_latest:    # type: ignore
                    items.append(sc0.items.SiteMapItem(loc=loc,current_latest=False))
                    self.logger.info("SITEMAP current_latest FLAG SET TO FALSE but scraping the sitemap anyway to update: %s ",loc)
                    requests.append(scrapy.Request(loc, callback=self.parse_period, meta={"current_latest":False}))
        requests.append(scrapy.Request(newest_sitemap_loc, callback=self.parse_period, meta={"current_latest":True}))
        return requests,items

    @abstractmethod
    def parse_period(self, response):
        pass

    def parse_content(self,response,**kwargs):
        self.logger.debug("CONTENTPARSE: %s ",response.url)
        text = trafilatura.baseline(response.text)[1]
        if not self.scrape_unscraped_db:
            lastmod = response.meta["lastmod"]
            yield sc0.items.UrlItem(text_content=text, lastmod=lastmod, url=response.url,scraped=True)
        else:
            yield sc0.items.UrlItem(text_content=text, url=response.url,scraped=True)

    def sigterm_handler(self, signum, frame):
        self.logger.info("SIGTERM received. Closing spider")
        sm = SignalManager()
        sm.send_catch_log(scrapy.signals.spider_closed)
        
        

class TbmmTutanakSpider(scrapy.Spider):
    name = 'tbmm'
    base_href = "https://www.tbmm.gov.tr"
    start_urls = [
        'https://www.tbmm.gov.tr/Tutanaklar/TutanakMetinleri']
    scraped_url= 0 
    in_memory_item_max = 100
    def parse(self, response, **kwargs):
        soup = bs4.BeautifulSoup(response.text, 'lxml')
        elements = soup.find_all("table", {"class": "table table-striped"})
        if elements:
            for element in elements:
                links = element.find_all("a", {"href": True})
                for link in links:
                    yield scrapy.Request(self.base_href + link.get("href"))
        else:
            # yield {"url": response.url}
            table_element = soup.find("table", {"class":"table"})
            if isinstance(table_element, bs4.Tag):
                rows =table_element.find_all("tr")
                for row in rows:
                    first_href = next(row.children, None)
                     #yield tutanak link
                    self.scraped_url += 1
                    self.logger.info("YIELDING TBMM RECORD TO SCRAPE")
                    if(first_href !=None):
                        yield scrapy.Request(response.urljoin(first_href.find("a", {"href":True}).get("href")), callback=self.parse_tutanak)

    def parse_tutanak(self, response, **kwargs):
        self.logger.debug("CONTENTPARSE: %s ",response.url)
        text = trafilatura.baseline(response.text)[1]
        yield sc0.items.UrlItem(text_content=text, url=response.url,scraped=True)
        # soup = bs4.BeautifulSoup(response.text, 'lxml')
        # element = soup.find("body")
        # self.logger.debug("SCRAPING TBMM RECORD")
        # if isinstance(element, bs4.PageElement):
        #     self.scraped_url += 1
        #     yield {"url": response.url, "text": element.text}
    
class SozcuSitemap(CustomSitemapSpider):
    name="sozcu"
    def __init__(self, name=name,autoscrape=False,scrape = False, **kwargs):
        """ Parse content from https://www.sozcu.com.tr
        Args:
            autoscrape (bool, optional): Defaults to False. If True, will scrape the content pages automatically.
            scrape_unscraped_db (bool, optional): Defaults to False. If True, will scrape the  
            unscraped content pages that are stored in the database instead of crawling the sitemap for new content pages.
        """
        super().__init__(name=name, **kwargs)
        self.db = sc0.database_obs.DatabaseObserver(database_uri="sqlite:///"+ "sozcu" +".db")
        if scrape:
            self.start_urls = self.db.get_unscraped_urls_as_str()
        else:
            self.logger.info("scrape=False so only the sitemaps will be scraped to update URL database")
            self.start_urls = ["https://www.sozcu.com.tr/tools/sitemaps/x/sitemapindex_all.php"]
        
        if autoscrape:
            self.logger.warning("Scraping sitemaps first is better for pages with sitemaps.")
        self.logger.info("Spider %s started", self.name)
        self.autoscrape = autoscrape
        self.scrape_unscraped_db = scrape

    def parse(self,response,**kwargs):
        """ Entry parse function that will yield requests for the sitemap periods.
        Yields:
            scrapy.Request : Request for the sitemap period
        """
        if self.scrape_unscraped_db:
            if response.url in self.start_urls:
                yield scrapy.Request(response.url, callback=self.parse_content)
        else:
            self.logger.info("INITIALPARSE: %s ",response.url)
            soup = bs4.BeautifulSoup(response.text,features="xml")
            locs = soup.findAll("loc")
            locs_str = [x.text for x in locs]
            loc_newest = locs_str[1]
            loc_others = locs_str[2:]
            reqs  , items = self.reqs_items_from_sitemapindex(loc_newest,loc_others)
            for item in items:
                yield item
            for req in reqs:
                yield req
            


    def parse_period(self,response,**kwargs):
        """ This function will parse the sitemap period and yield requests for the content pages.

        Args:
            response (_type_): _description_

        Yields:
            _type_: _description_
        """
        self.logger.info("PERIODPARSE: %s ",response.url)
        
        soup = bs4.BeautifulSoup(response.text,features="xml")
        urls = soup.findAll("url")
        sitemap_lastUrl = urls[0].find("loc").text
        sitemap_current_latest = response.meta["current_latest"]
        for url in urls:
            loc = url.find("loc")
            lastmod = url.find("lastmod")
            lastmod_as_datetime = datetime.datetime.strptime(lastmod.text, '%Y-%m-%dT%H:%M:%S%z')

            if self.autoscrape:
                yield scrapy.Request(loc.text, callback=self.parse_content,meta={"lastmod":lastmod_as_datetime})
            else:
                yield sc0.items.UrlItem(url=loc.text,lastmod=lastmod_as_datetime,scraped = False)
            
        self.logger.info("Looped through all urls and yielded a content parse request for %s", response.url)
        self.logger.info("Adding sitemap to db")
        yield sc0.items.SiteMapItem(loc=response.url,lastUrl=sitemap_lastUrl,current_latest = sitemap_current_latest)
        
class EnSonHaberSitemap(CustomSitemapSpider):
    name="ensonhaber"
    def __init__(self, name=None,autoscrape=False,scrape = False, **kwargs):
        """ Parse content from https://www.ensonhaber.com.tr
        Args:
            autoscrape (bool, optional): Defaults to False. If True, will scrape the content pages automatically.
            scrape_unscraped_db (bool,
        """
        super().__init__(name=name, **kwargs)
        self.db = sc0.database_obs.DatabaseObserver(database_uri="sqlite:///"+ "ensonhaber" +".db")
        if scrape:
            self.start_urls = self.db.get_unscraped_urls_as_str()
        else:
            self.start_urls = ["https://www.ensonhaber.com/sitemap/haberler.xml"]
        
        if autoscrape:
            self.logger.warning("Scraping sitemaps first is better for pages with sitemaps.")
        self.logger.info("Spider %s started", self.name)
        self.autoscrape = autoscrape
        self.scrape_unscraped_db = scrape
        
    def parse(self,response,**kwargs):
        if self.scrape_unscraped_db:
            if response.url in self.start_urls:
                yield scrapy.Request(response.url, callback=self.parse_content)
        else:
            #log the loggers name 
            self.logger.info("Loggers name: %s",self.logger.name)
            self.logger.info("rwar1INITIALPARSE: %s ",response.url)
            soup = bs4.BeautifulSoup(response.text,features="xml")
            locs = soup.findAll("loc")
            locs_str = [x.text for x in locs]
            loc_newest = locs_str[0]
            loc_others = locs_str[1:]
            reqs  , items = self.reqs_items_from_sitemapindex(loc_newest,loc_others)
            for item in items:
                yield item
            for req in reqs:
                yield req
    
    def parse_period(self, response):
        self.logger.info("PERIODPARSE: %s ",response.url)
        soup = bs4.BeautifulSoup(response.text,features="xml")
        urls = soup.findAll("url")
        try:
            sitemap_lastUrl = urls[0].find("loc").text
        except IndexError as ie: 
            self.logger.error("IndexError: %s",ie)
            sitemap_lastUrl = "NOURL"
        sitemap_current_latest = response.meta["current_latest"]
        for url in urls:
            loc = url.find("loc")
            loc_text= loc.text.strip()
            lastmod = url.find("lastmod")
            lastmod_as_datetime = datetime.datetime.strptime(lastmod.text, '%Y-%m-%dT%H:%M:%S%z')

            if self.autoscrape:
                yield scrapy.Request(loc_text, callback=self.parse_content,meta={"lastmod":lastmod_as_datetime})
            else:
                yield sc0.items.UrlItem(url=loc_text,lastmod=lastmod_as_datetime,scraped = False)
            
        self.logger.info("Looped through all urls and yielded a content parse request for %s", response.url)
        self.logger.info("Adding sitemap to db")
        yield sc0.items.SiteMapItem(loc=response.url,lastUrl=sitemap_lastUrl,current_latest = sitemap_current_latest)

class HaberlerSitemap(EnSonHaberSitemap):
    name="haberler"
    def __init__(self, name=None,autoscrape=False,scrape = False, **kwargs):
        super().__init__(name, autoscrape,scrape)
        self.db = sc0.database_obs.DatabaseObserver(database_uri="sqlite:///"+ "haberler" +".db")
        if scrape:
            self.start_urls = self.db.get_unscraped_urls_as_str()
        else:
            self.start_urls = ["https://www.haberler.com/sitemap_news.xml"]

    def parse_period(self, response):
        self.logger.info("PERIODPARSE: %s ",response.url)
        soup = bs4.BeautifulSoup(response.text,features="xml")
        urls = soup.findAll("url")
        try:
            sitemap_lastUrl = urls[0].find("loc").text
        except IndexError as ie: 
            self.logger.error("IndexError: %s",ie)
            sitemap_lastUrl = "NOURL"
        sitemap_current_latest = response.meta["current_latest"]
        for url in urls:
            loc = url.find("loc")
            loc_text= loc.text.strip()
            lastmod = url.find("lastmod")
            

            if self.autoscrape:
                yield scrapy.Request(loc_text, callback=self.parse_content)
            else:
                yield sc0.items.UrlItem(url=loc_text,scraped = False)
            
        self.logger.info("Looped through all urls and yielded a content parse request for %s", response.url)
        self.logger.info("Adding sitemap to db")
        yield sc0.items.SiteMapItem(loc=response.url,lastUrl=sitemap_lastUrl,current_latest = sitemap_current_latest)
        