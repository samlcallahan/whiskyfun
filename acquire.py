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

URL = "http://www.whiskyfun.com/"

thread_local = threading.local()

def get_session():
    '''
    if the current thread doesn't have a requests Session object, creates one and updates the headers
    '''

    if not hasattr(thread_local, "session"):
        thread_local.session = Session()
        thread_local.session.headers.update(HEADERS)
    return thread_local.session

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
    session = get_session()

    soup = BeautifulSoup(session.get(URL).content)
    
    right_fonts = soup.find_all('font', color='#666666', size='2', face='Arial')

    links = []

    for html_blob in right_fonts:
        if html_blob.find('a'):
            links.append(html_blob.find('a')['href'])

    archives = []

    for link in links:
        if "archive" in link.lower() or 'feisile' in link.lower() or link.lower() == 'special.html':
            archives.append(link)

    archives = pd.Series(archives)

    archives = archives.apply(make_full_link)

    return archives

def scrape_page(archive_url):
    session = get_session()

    soup = BeautifulSoup(session.get(URL).content)

    soup.find('table', width='540', border='0', align='center', cellpadding='0', cellspacing='0')