import re
import time
import pandas as pd
from io import StringIO
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


class GoodInfoScraper:
    BASE_URL = "https://goodinfo.tw/tw/StockDetail.asp?STOCK_ID="

    def __init__(self, ticker: str, wait_sec: int = 5):
        self.ticker = ticker
        self.wait_sec = wait_sec
        self.page_source = ""
        self.soup = None
        self.update_date = None
        self.data1 = None
        self.data2 = None
        self.data3 = None
        self.info = {}
        self.df1 = pd.DataFrame()
        self.df2 = pd.DataFrame()

    def _get_driver(self):
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
        return webdriver.Chrome(options=options)

    def fetch(self):
        driver = self._get_driver()
        driver.get(f"{self.BASE_URL}{self.ticker}")
        time.sleep(self.wait_sec)
        self.page_source = driver.page_source
        driver.quit()
        self.soup = BeautifulSoup(self.page_source, "html.parser")

    def extract_update_date(self):
        nobr_tags = self.soup.select("table.b0.p0 nobr")
        for tag in nobr_tags:
            if re.search(r'\b\d{2}/\d{2}/\d{2}\b', tag.text):
                raw_date = tag.text.strip().strip("'")
                raw_date = re.sub(r'\xa0.*', '', raw_date)
                self.update_date = datetime.strptime(raw_date, '%y/%m/%d')
                return self.update_date
        return None

    def extract_tables(self):
        self.data1 = self.soup.select_one('table.b0v1h0.p5_4.row_bg_2N.row_mouse_over')
        self.data2 = self.soup.select_one('table.b1000v1h0.p5_0.row_bg_2N.row_mouse_over')
        for table in self.soup.select('table.b0v1h1.p4_4'):
            if set(table.get('class', [])) == {'b0v1h1', 'p4_4'}:
                self.data3 = table
                break

    def parse_company_info(self):
        table = self.data3
        info = {}
        last_key = None
        for row in table.find_all('tr'):
            cells = row.find_all(['th', 'td'])
            if not cells:
                continue
            if len(cells) == 1 and cells[0].name == 'th':
                last_key = cells[0].get_text(strip=True)
                continue
            if len(cells) == 1 and cells[0].name == 'td' and last_key:
                info[last_key] = cells[0].get_text(" ", strip=True)
                continue
            i = 0
            while i < len(cells):
                if cells[i].name == 'th':
                    key = cells[i].get_text(strip=True)
                    i += 1
                    if i < len(cells):
                        val = cells[i].get_text(" ", strip=True)
                        info[key] = val
                i += 1
        self.info = info
        return info

    def extract_dataframes(self):
        dfs1 = pd.read_html(StringIO(self.data1.prettify()))
        dfs2 = pd.read_html(StringIO(self.data2.prettify()))
        self.df1 = dfs1[0].T.copy()
        self.df2 = dfs2[0].copy()
        return self.df1, self.df2

    def run(self):
        
        self.fetch()
        self.extract_update_date()
        self.extract_tables()
        self.parse_company_info()
        self.extract_dataframes()
