import sqlite3
import sys
class DatabaseBrowser:
    def __init__(self,db_path) -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    def get_urls_scraped(self):
        self.cursor.execute("SELECT * FROM urls WHERE scraped = 1")
        return self.cursor.fetchall()
    
    def get_all_urls(self):
        self.cursor.execute("SELECT * FROM urls")
        return self.cursor.fetchall()
        
if __name__ =="__main__":
    #get the first argument from command line
    db_path = input("Enter the path to the database: ")
    #connect to database
    db = DatabaseBrowser(db_path)
    scraped_urls = db.get_urls_scraped()
    all_urls = db.get_all_urls()
    print("Scraped url count " + str(len(scraped_urls)))
    print("All url count " + str(len(all_urls)))

