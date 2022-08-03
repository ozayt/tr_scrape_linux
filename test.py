import sc0.database_obs
import sqlalchemy.orm
from sc0.models import SiteMaps, Url, create_table, get_engine
import trafilatura
import bs4
import datetime
import requests

db = sc0.database_obs.DatabaseObserver(echo=True,database_uri="sqlite:///sc0/data.db")
sesion = db.Session()
#TESTS 
# q = [obj[0] for obj in sesion.query(Url.url).filter(Url.scraped == False).all()]

def test_xml_response_bs4(url ="https://www.sozcu.com.tr/tools/sitemaps/x/sitemapindex_all.php"):
    #defult : 
    response = requests.get(url)
    soup = bs4.BeautifulSoup(response.text,features="xml")
    locs = soup.findAll("loc")
    return locs

def test_trafilatura_bare(url="https://www.sozcu.com.tr/hayatim/magazin-haberleri/unlu-sarkici-shakira-icin-8-yil-hapis-cezasi-isteniyor/?utm_source=anasayfa&utm_medium=free&utm_campaign=alt_surmanset"):
    response = requests.get(url)
    #get html from response as string
    
    html = response.text
    now = datetime.datetime.now()
    trafilatura.extract(html)
    elapsed_time_ms = (datetime.datetime.now() - now).total_seconds() * 1000
    print("Extract elapsed time: %s ms" % elapsed_time_ms)
    now = datetime.datetime.now()
    trafilatura.baseline(html)
    elapsed_time_ms = (datetime.datetime.now() - now).total_seconds() * 1000
    print("Baseline extract elapsed time: %s ms" % elapsed_time_ms)

    return trafilatura.baseline(html)

