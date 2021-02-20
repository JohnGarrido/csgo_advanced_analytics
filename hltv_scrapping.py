import requests
from bs4 import BeautifulSoup
import re 
import pandas as pd
import bs4
import numpy as np

# Defining headers to BeautiffulSoup

headers = {"User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36"}  

# Team ID to refer

TEAM_ID = "8297"

# The team's match page is splitted out "offsets", the current threshold is 300 for average and to avoid possible old demos issues

OFFSETS = [0, 100, 200, 300]

URLs = []

for offset in OFFSETS:

    # For each of set, it creates an url where we can find the matches links
    url = "https://www.hltv.org/results?offset="+str(offset)+"&content=demo&content=vod&team="+TEAM_ID
    URLs.append(url)

    for URL in URLs:
        
        # Parsing pages html as string

        page = requests.get(URL,headers=headers)
        soup = BeautifulSoup(page.content, 'html.parser')

        html = str(soup)

        links = []

        ref = html.split('/matches/')[1:]

        for value in ref:
            sublink = value.split('"')[0]
            links.append("https://www.hltv.org/matches/"+sublink)

        df = pd.DataFrame(links, columns=['Link'])
        
download_links = []
dates = []
events = []
versus = []

for link in df['Link'].values:  

    page = requests.get(link, headers=headers)
    soup = BeautifulSoup(page.content, 'html.parser')
    try:
        download_link = str(soup).split('"flexbox left-right-padding" href="')[1].split('">')[0]
        download_links.append("https://www.hltv.org"+download_link)

        dates_link = str(soup).split('MMMM y" data-unix="')[1].split(">")[1].split("<")[0]
        dates.append(dates_link)

        event = str(soup).split('<div class="event text-ellipsis"><a href=')[1].split('title="')[1].split('">')[0]
        events.append(event)

        team_1 = str(soup).split('team1-gradient"')[1].split('alt="')[1].split('"')[0]
        team_2 = str(soup).split('team2-gradient"')[1].split('alt="')[1].split('"')[0]

        versus.append(team_1 +" vs. "+team_2)
    except:
        download_links.append(np.nan)
        dates.append(np.nan)
        events.append(np.nan)
        versus.append(np.nan)
        
df['Download'] = download_links
df['Event'] = events
df['Date'] = pd.to_datetime(dates) 
df['Match'] = versus

df.sort_values(by='Date', ascending=False, inplace=True)

df.dropna(inplace=True)

df.to_csv('data/downloads/furia_games_hltv.csv')