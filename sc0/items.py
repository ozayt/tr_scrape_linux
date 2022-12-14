# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class UrlItem(scrapy.Item):
    """Item that contains the url and text_content of a page
    """
    text_content = scrapy.Field()
    lastmod = scrapy.Field()
    url = scrapy.Field()
    scraped = scrapy.Field()

class SiteMapItem(scrapy.Item):
    loc = scrapy.Field()
    lastUrl = scrapy.Field()
    current_latest = scrapy.Field()
    
    



    
