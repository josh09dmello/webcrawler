import requests
from bs4 import BeautifulSoup
import csv

# Define file names
input_csv_file = 'delhi_property_listings.csv'
output_csv_file = 'delhi_extracted_property_data.csv'

# Step 1: Scrape property listing links and save to CSV
def scrape_property_listings():
    with open(input_csv_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Project Data Link Name", "Listing Href Link"])

    for i in range(1, 2820):
        url = 'https://www.makaan.com/delhi-residential-property/buy-property-in-delhi-city?page=' + str(i)
        page = requests.get(url, timeout=20)
        soup = BeautifulSoup(page.text, 'html.parser')

        for each_text in soup.find_all('div', {"class": "title-line-wrap"}):
            listing_href_link = ''
            project_data_link_name = ''

            # Extract data-link-name and href from the listing link
            listing_link = each_text.find('a', {"data-type": "listing-link"})
            if listing_link:
                listing_href_link = listing_link.get('href')

            # Extract data-link-name from the project name link
            project_link = each_text.find('a', {"class": "projName"})
            if project_link:
                project_data_link_name = project_link.get('data-link-name')

            # Append the data to the CSV file
            with open(input_csv_file, mode='a', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow([project_data_link_name, listing_href_link])

# Step 2: Extract detailed property data for each listing
def extract_property_data(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Extract price
    price_meta = soup.find('meta', itemprop='price')
    price = price_meta['content'] if price_meta else None
    
    # Extract property name
    name_span = soup.find('span', class_='ib')
    name = name_span.find('span', itemprop='name').text if name_span else None
    
    # Extract latitude and longitude
    geo_div = soup.find('div', itemprop='geo')
    latitude = geo_div.find('meta', itemprop='latitude')['content'] if geo_div else None
    longitude = geo_div.find('meta', itemprop='longitude')['content'] if geo_div else None
    
    # Extract status
    status_td = soup.find('td', id='Status')
    status = status_td.get('title') if status_td else None
    
    # Extract rate per square feet
    rate_wrap = soup.find('div', class_='rate-wrap')
    rate = rate_wrap.text.strip() if rate_wrap else None
    
    # Extract age of property
    age_td = soup.find('td', id='Age of Property', class_='val')
    age = age_td.get('title') if age_td else None
    
    return {
        'Price': price,
        'Name': name,
        'Latitude': latitude,
        'Longitude': longitude,
        'Status': status,
        'Rate per Sq Ft': rate,
        'Age of Property': age
    }

# Step 3: Read the input CSV file and extract property details
def extract_and_save_property_details():
    with open(input_csv_file, mode='r', newline='', encoding='utf-8') as infile, \
         open(output_csv_file, mode='w', newline='', encoding='utf-8') as outfile:
        
        reader = csv.DictReader(infile)
        fieldnames = ['Price', 'Name', 'Latitude', 'Longitude', 'Status', 'Rate per Sq Ft', 'Age of Property']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        
        writer.writeheader()
        
        for row in reader:
            url = row['Listing Href Link']
            data = extract_property_data(url)
            writer.writerow(data)

# Run both scraping and data extraction functions
scrape_property_listings()
extract_and_save_property_details()
