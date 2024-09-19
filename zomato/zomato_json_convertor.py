import os
import json
import csv

# Define the root directory and city name
ZomataDataRoot = '/Users/joshua.d/Downloads/drive-download-20240730T143307Z-001/Zomato Scraping/lucknow'
city_name = 'Lucknow'

def getDataFromFile(path):
    output = []
    with open(path, 'r') as inFile:
        for line in inFile:
            txt = line.replace('\n', '')
            if txt not in ['null', '']:
                # Try loading as json
                obj = json.loads(txt)
                output.append(obj)
    return output

records = []
listOfFiles = os.listdir(ZomataDataRoot)
for fl in listOfFiles:
    print(fl)
    file_path = os.path.join(ZomataDataRoot, fl)
    rows = getDataFromFile(file_path)
    records.extend(rows)  # Use extend to add multiple rows

ZeroLevelKeys = ['name', 'url', 'telephone', 'servesCuisine', 'priceRange']
AddressKeys = ['streetAddress', 'addressLocality', 'addressRegion', 'postalCode', 'addressCountry']
GeoKeys = ['latitude', 'longitude']

formattedRecords = []
for r in records:
    ob = {}
    for k in ZeroLevelKeys:
        ob[k] = r.get(k, '')  # Use get with default empty string to avoid KeyError

    for k in AddressKeys:
        if 'address' in r:
            ob[k] = r['address'].get(k, '')
        else:
            ob[k] = r.get(k, '')
    for k in GeoKeys:
        if 'geo' in r:
            ob[k] = r['geo'].get(k, '')
        else:
            ob[k] = r.get(k, '')
    formattedRecords.append(ob)

# Save data as CSV
headers = ZeroLevelKeys + AddressKeys + GeoKeys

with open(f'Zomato_{city_name}.csv', 'w', encoding='UTF-8', newline='\n') as outFile:
    writer = csv.DictWriter(outFile, fieldnames=headers, delimiter='|')
    writer.writeheader()
    writer.writerows(formattedRecords)
