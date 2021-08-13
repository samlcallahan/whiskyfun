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

HEADERS = {'User-Agent': user}

import format1
import format2
import format3
import feisile
import format4

HEADERS = {'User-Agent': user}

URL = "http://www.whiskyfun.com/"

thread_local = threading.local()

def make_full_link(link_url):
    '''
    Checks if url is full url or url stub. converts to full url if not already.
    '''
    if link_url[:4] != 'http':
        link_url = URL + link_url
    return link_url

def archive_list(main_url):
    '''
    Gets a list of each archive page from the main site
    '''
    session = Session()

    session.headers.update(HEADERS)

    soup = BeautifulSoup(session.get(URL).content)
    
    right_fonts = soup.find_all('font', color='#666666', face='Arial')

    links = []

    for html_blob in right_fonts:
        if html_blob.find('a'):
            links.append(html_blob.find('a')['href'])

    archives = [URL]

    for link in links:
        if "archive" in link.lower() or 'feisile' in link.lower() or link.lower() == 'special.html':
            archives.append(link)

    archives = pd.Series(archives)

    archives = archives.apply(make_full_link)

    return archives

def all_pages(archives):
    for i, archive in enumerate(archives):
        if archive == 'http://www.whiskyfun.com/archivedecember17-1-Ardbeg-Chichibu-Ledaig.html':
            archives = archives.iloc[i:]
            break
        format1.scrape_page(archive)

    for i, archive in enumerate(archives):
        if archive == 'http://www.whiskyfun.com/archivedecember09-1.html':
            archives = archives.iloc[i:]
            break
        format2.scrape_page(archive)
    
    for i, archive in enumerate(archives):
        if archive == 'http://www.whiskyfun.com/ArchiveSeptember04.html':
            archives = archives.iloc[i:]
            break
        if archive == 'http://www.whiskyfun.com/special.html':
            feisile.scrape_page(archive)
            continue
        format3.scrape_page(archive)  
    
    for archive in archives:
        if archive == 'http://www.whiskyfun.com/ArchiveJan04.html':
            break
        format4.scrape_page(archive)

def combine_feathers():
    df = pd.DataFrame()
    for feather in os.listdir('data'):
        to_append = pd.read_feather(f'data/{feather}')
        df = df.append(to_append, ignore_index=True)
    return df

def whisky_df():
    archives = archive_list(URL)
    all_pages(archives)
    whisky = combine_feathers()
    whisky.to_csv('whiskyfun.csv')
    return whisky