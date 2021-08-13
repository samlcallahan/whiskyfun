import pandas as pd
from bs4 import BeautifulSoup
from requests import get, Session
import wikipedia as wiki
import pyarrow.feather as feather
import os
import sys
from env import user
from time import time
import time
import concurrent.futures
import threading
import re
from acquire import HEADERS

def data(soup):
    content_soups = soup.find_all('font', size = '2', face = 'Verdana')
    content_soups = [x for x in content_soups if 'Colour:' in x.text]

    data = []

    for content_soup in content_soups:
        data.append({'content': content_soup.text,
                     'title': 'feisile',
                     'rating': 'feisile',
                     'date': content_soup.find_previous('font', size='3').text})

    return data

def scrape_page(archive_url):
    print(f'scraping {archive_url}')
    session = Session()

    session.headers.update(HEADERS)

    soup = BeautifulSoup(session.get(archive_url).content, 'html.parser')

    page = pd.DataFrame(data(soup))

    page['author'] = 'Serge'

    feather.write_feather(page, f'data/{archive_url[25:-5]}.feather')