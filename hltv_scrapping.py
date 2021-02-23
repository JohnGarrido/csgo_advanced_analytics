import pandas as pd
import os 
import requests
from bs4 import BeautifulSoup
import re 
import pandas as pd
import numpy as np

HLTV = 'data/downloads/furia_games_hltv.csv'

def get_team_matches():

    # Defining headers for BeautiffulSoup
    headers = {"User-Agent":
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36"}  

    # Team ID to refer
    TEAM_ID = "8297"

    # The team's match page is splitted through "offsets", the current threshold is 300 for average and to avoid possible old demos issues
    OFFSETS = [0, 100, 200, 300]

    URLs = []
    matches = []

    for offset in OFFSETS:

        # For each of set, it creates an url where we can find the matches links
        url = "https://www.hltv.org/results?offset="+str(offset)+"&content=demo&content=vod&team="+TEAM_ID
        URLs.append(url)

    for URL in URLs:

        page = requests.get(URL,headers=headers)
        soup = BeautifulSoup(page.content, 'html.parser')

        html = soup.find_all(class_='result-con')

        offset_matches = re.findall(r'href=\"/matches/(.*?)\"', str(html))
        matches.extend(offset_matches)

    links = ["https://www.hltv.org/matches/"+sublink for sublink in matches]

    df = pd.DataFrame(links, columns=['Link'])
    df.drop_duplicates(inplace=True) # for some reason there's a value being duplicated during the offset parsin
    
    return df

def create_hltv(matches):
    
    # Defining headers for BeautiffulSoup
    headers = {"User-Agent":
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36"}  

    df = matches

    download_links = []
    dates = []
    events = []
    versus = []
    picks = []

    step = 0
    stages = len(df['Link'].values)

    print("{} matches found, starting to build HLTV\n".format(stages))

    for link in df['Link'].values:  

        page = requests.get(link, headers=headers)
        soup = BeautifulSoup(page.content, 'html.parser')

        try:

            # Scraping download links
            download_link = re.findall(r'href=\"/download/(.*?)\"', str(soup.find_all(class_='stream-box')))[0]
            download_links.append("https://www.hltv.org/download/"+download_link+"/")

            # Scraping match dates
            date = re.findall(r'>(.*?)</div>', str(soup.find_all(class_='date')))[0]
            dates.append(date)

            # Scraping event name
            event = re.findall(r'title="(.*?)">', str(soup.find_all(class_='event text-ellipsis')))[0]
            events.append(event)

            # Scraping teams and setting up a "versus" format 
            team_1 = re.findall(r'title="(.*?)"/>', str(soup.find_all(class_='team1-gradient')))[0]
            team_2 = re.findall(r'title="(.*?)"/>', str(soup.find_all(class_='team2-gradient')))[0]

            versus.append(team_1 +" vs. "+team_2)

            # Scraping maps played
            maps = list(set([x.capitalize() for x in re.findall(r'>(.*?)<', 
                                                                str(soup.find_all(class_='stats-menu-link')))[2:] if len(x) > 3]))
            picks.append(', '.join(map(str, maps)))

        except:
            
            download_links.append(np.nan)
            dates.append(np.nan)
            events.append(np.nan)
            versus.append(np.nan)
            picks.append(np.nan)

        step += 1
        print("Table {:.1f}% completed...".format(step/stages * 100))

    df['Download'] = download_links
    df['Event'] = events
    df['Date'] = pd.to_datetime(dates) 
    df['Match'] = versus
    df['Maps'] = picks

    df = df[['Match', 'Maps', 'Date', 'Event','Link', 'Download']]

    return df

def add_new_matches():
    
    hltv = pd.read_csv(HLTV)

    CURRENT_MATCHES = hltv['Link'].to_list()

    matches = get_team_matches()['Link'].to_list()
    outer_matches = [x for x in matches if x not in CURRENT_MATCHES]

    if(outer_matches == []):
        print("There's no new matches to be added!")

    else:
        new_matches = pd.DataFrame(outer_matches, columns=['Link'])
        build_new_matches = create_hltv(new_matches)

        df = pd.concat([build_new_matches, hltv])
        df = df[['Match', 'Maps', 'Date', 'Event','Link', 'Download']]
        df.to_csv(HLTV, index=False)

    for match in outer_matches:
        print("{} was added to HLTV".format(match.split('/')[-1]))
        
    return "Finished!"

if os.path.exists(HLTV):
    add_new_matches()
    
else:
    table = create_hltv(get_team_matches())
    table.to_csv(HLTV)


