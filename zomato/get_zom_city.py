import requests
from bs4 import BeautifulSoup
import json
import os
import csv
import random
import time

# Specify the city name here
CITY_NAME = "lucknow"
BASE_URL = f"https://www.zomato.com/{CITY_NAME}/"

# Function to get userAgents
def getUserAgents():
    UAs = readLinesFromFile('userAgents.csv')
    return UAs

def readLinesFromFile(path):
    data = []
    with open(path, 'r') as inFile:
        for line in inFile:
            data.append(line.replace('\n', ''))
    return data

def saveListToFile(lst, path):
    with open(path, 'w', newline='\n', encoding='UTF-8') as outFile:
        for l in lst:
            outFile.write(l + '\n')

def getRandomUA():
    return random.choice(userAgents)

def getDirectory(BASE_URL):
    ua = getRandomUA()
    headers = {'User-Agent': ua}
    URL = f"{BASE_URL}directory"
    response = requests.get(URL, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')
    divs = soup.find_all(class_='restaurant_pagination pagination')

    infos = []
    for d in divs:
        aTags = d.find_all('a')
        for atag in aTags:
            infos.append(atag.attrs['href'])
    return infos

def getRestaurantURL(url):
    ua = getRandomUA()
    headers = {'User-Agent': ua}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')
    divs = soup.find_all(class_='plr10')

    infos = []
    for d in divs:
        aTag = d.find('a')
        keys = list(aTag.attrs.keys())
        if 'title' in keys and 'href' in keys:
            infos.append(aTag.attrs)

    return infos

def getRestInfo(url):
    ua = getRandomUA()
    headers = {'User-Agent': ua}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')
    scriptTags = soup.find_all('script')
    bdy = None
    for s in scriptTags:
        if(len(s.contents) == 1):
            txt = s.contents[0]
            if (txt.startswith('{')) and (txt.endswith('}')):
                bdy = json.loads(txt)
                if('@type' in bdy) and (bdy['@type'] == "Restaurant"):
                    # print(bdy)
                    break
                else:
                    bdy = None
    return bdy

def createDirectoryIfNotExists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

# Load user agents
userAgents = getUserAgents()

# Get the Directory
directoryList = getDirectory(BASE_URL)

# Save to file
saveListToFile(directoryList, f'{CITY_NAME}_nm_lst.txt')

# Ensure the city directory exists
createDirectoryIfNotExists(CITY_NAME)

for d in directoryList:
    partName = d.split('/').pop()
    fileName = f'{partName}.txt'
    filePath = os.path.join(CITY_NAME, fileName)

    if not os.path.exists(filePath):
        # Get the list of restaurants
        restaurants = getRestaurantURL(d)
        resCount = len(restaurants)
        print(f"Got list for: {d}")
        resInfo = []
        idx = 0
        for r in restaurants:
            url = r['href']
            # Get the restaurant information
            thisRestaurantInfo = getRestInfo(url)
            txt_op = json.dumps(thisRestaurantInfo)
            resInfo.append(txt_op)
            # Sleep
            time.sleep(0.5)
            idx += 1
            print(f"{idx}/{resCount}")
        # Save this in the output file
        saveListToFile(resInfo, filePath)
