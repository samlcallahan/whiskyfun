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

# format 2 example website: http://www.whiskyfun.com/archivejune17-2-Ardmore-Port-Ellen-Caol-Ila.html

def is_title(soup_element):
    '''
    checks if a soup element is a title.
    in this format, titles are span elements with the class 'textegrandfoncegras'
    and contain a () and a % to denote the abv of the spirit.
    '''
    if soup_element.name == 'span' and soup_element.get('class') == ['textenormalfoncegras'] and '(' in soup_element.text and '%' in soup_element.text:
        return True
    else:
        return False

def is_date(soup_element):
    '''
    checks if a soup element is a date.
    these are all font elements with color #660000 and size 3
    '''
    if soup_element.name == 'font' and soup_element.get('color') == '#660000' and soup_element.get('size') == '3':
        return True
    else:
        return False

def angus_list(page_soup):
    '''
    returns a list of all sections of soup sections written by Angus instead of Serge
    '''

    # all of angus's sections are written in slightly narrower tables
    angus_sections = page_soup.find_all('table', width='498')

    # iterates though all of the sections that contain reviews by angus
    # adds all the titles found in angus sections to the angus list
    angus = []

    for section in angus_sections:
        angus_soups = section.find_all('span', class_='textegrandfoncegras')
        angus.extend([title.text for title in angus_soups if is_title(title)])

    return angus

def author_by_title(title, angus_list):
    '''
    checks if a title is in the angus_list
    returns 'Angus' if it is and 'Serge' if it's not
    '''
    if title in angus_list:
        return 'Angus'
    
    else:
        return 'Serge'

def rating(title_soup):
    '''
    this format is actually inconsistent (shocker)
    sometimes serge doesn't change the font/make a new html element for his rating, so 
    it winds up in the content. this function will check if there's a separate rating element
    after the title_soup passed in. if it is a rating, it'll return the rating text,
    else it returns None
    '''

    rating_soup = title_soup.find_next(class_='textenormalgras')
    if rating_soup:
        return rating_soup.text
    else:
        return None

def data(soup):
    '''
    Extracts the data from the page soup
    Gets content, title, date, and rating
    returns as a list of dictionaries
    '''

    # gets all soup bits that are review titles
    title_soups = soup.find_all(lambda x: is_title(x))

    # iterates through each piece of title soup
    # appends to the data list the content, title, date, and rating
    # content is contained in the parent soup element
    # date is the most previous soup element that matches the is_date conditions
    # rating is the next soup element that matches the is_rating conditions
    data = []

    for soup in title_soups:
        data.append({'title': soup.text,
                     'date': soup.find_previous(lambda x: is_date(x)).text,
                     'rating': rating(soup),
                     'content': soup.parent.text})
    
    return data
    
def scrape_page(archive_url):
    print(f'scraping {archive_url}')
    session = Session()

    session.headers.update(HEADERS)

    soup = BeautifulSoup(session.get(archive_url).content, 'html.parser')

    page = pd.DataFrame(data(soup))

    angus_titles = angus_list(soup)

    page['author'] = page['title'].apply(lambda x: author_by_title(x, angus_titles))

    feather.write_feather(page, f'data/{archive_url[25:-5]}.feather')