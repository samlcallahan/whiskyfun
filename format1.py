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

def is_date(soup_element):
    if soup_element.name == 'font' and soup_element.get('color') == '#660000' and soup_element.get('size') == '3':
        return True
    else:
        return False

def is_title(soup_element):
    if soup_element.name == 'span' and soup_element.get('class') == ['textegrandfoncegras'] and '(' in soup_element.text and '%' in soup_element.text and soup_element.parent.find('img'):
        return True
    else:
        return False

def is_rating(soup_element):
    if soup_element.name == 'span' and soup_element.get('class') == ['textenormalgras'] and ' points' in soup_element.text:
        return True
    else:
        return False

def is_content(soup_element):
    if not soup_element.get('class') == ['TextenormalNEW']:
        return False
    if not 'Colour:' in soup_element.text:
        return False
    else:
        return True

def data(page_soup):
    content_soups = page_soup.find_all(lambda x: is_content(x))

    data = []
    for soup in content_soups:
        data.append({'content': soup.text,
                     'title': soup.find_previous().text,
                     'date': soup.find_previous(lambda x: is_date(x)).text,
                     'rating': soup.find_next(lambda x: is_rating(x)).text})
    return data

def angus_list(page_soup):
    angus_sections = page_soup.find_all('table', width='498')

    angus = []

    for section in angus_sections:
        angus_soups = section.find_all('span', class_='textegrandfoncegras')
        angus.extend([title.text for title in angus_soups if is_title(title)])

    return angus

def author_by_title(title, angus_list):
    if title in angus_list:
        return 'Angus'
    
    else:
        return 'Serge'

def contents_list(page_soup):
    content_soups = page_soup.find_all(class_='TextenormalNEW')

    contents = [content.text for content in content_soups if 'Colour:' in content.text]

    return contents

def scrape_page(archive_url):
    print(f'scraping {archive_url}')
    session = Session()

    session.headers.update(HEADERS)

    soup = BeautifulSoup(session.get(archive_url).content, 'html.parser')

    page = pd.DataFrame(data(soup))

    angus_titles = angus_list(soup)

    page['author'] = page['title'].apply(lambda x: author_by_title(x, angus_titles))

    feather.write_feather(page, f'data/{archive_url[25:-5]}.feather')