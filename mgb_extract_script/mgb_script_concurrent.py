import asyncio
from playwright.async_api import async_playwright
import csv
import re
import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import json
from pandas import json_normalize
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
import csv
import itertools
import re

# Paths and locality list
file_path = 'delhi_locality_script_contents_4.csv'
output_normalized_file = 'test1_new_merged_normalized_data.csv'
output_final_file = 'test1_new_merged_updated_data.csv'

# Semaphore to control the number of concurrent windows/tabs
max_concurrent_tasks = 5  # Adjust the number to control concurrency
semaphore = asyncio.Semaphore(max_concurrent_tasks)

# Integrate all steps
def extract_rent_and_save(df, output_file):
    fieldnames = df.columns.tolist() + ['extracted_element', 'property_price']

    with open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for _, row in df.iterrows():
            extracted_value, property_price = extract_rent_info(row)
            row['extracted_element'] = extracted_value
            row['property_price'] = property_price
            writer.writerow(row)


# Selenium function to extract rent information
def extract_rent_info(row):
    driver = webdriver.Chrome()
    driver.implicitly_wait(10)
    url = row['@id']

    try:
        driver.get(url)

        wait = WebDriverWait(driver, 20)
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        if iframes:
            driver.switch_to.frame(iframes[0])

        element_1 = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div/div/div/div[3]/div[2]/div/div/div[1]/section[1]/div/div[4]/div[2]/ul/li[1]/div[2]/div[2]')))
        extracted_value = element_1.text.strip()

        element_2 = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div/div/div/div[3]/div[2]/div/div/div[1]/section[1]/div/div[1]/div[1]')))
        property_price = element_2.text.strip()

        driver.quit()
        return extracted_value, property_price

    except TimeoutException:
        driver.save_screenshot("timeout_error.png")
        driver.quit()
        return None, None
    except Exception as e:
        driver.quit()
        return None, None
    
# Function to extract elements using Playwright
async def extract_elements(page, max_elements, locality):
    elements_count = 0
    retries = 3  # Number of retries if an element is not attached to the DOM
    max_failed_attempts = 20  # Maximum number of consecutive failed attempts
    failed_attempts = 0  # Counter for failed attempts

    for i in range(4, 1000):
        try:
            scroll_xpath = f"/html/body/div/div/div/div[2]/div[4]/div/div/div[1]/div[{i}]"
            script_xpath = f"/html/body/div/div/div/div[2]/div[4]/div/div/div[1]/div[{i}]/div/script[1]"

            found_element = False

            for attempt in range(retries):
                try:
                    scroll_locator = page.locator(f"xpath={scroll_xpath}")
                    if await scroll_locator.count() > 0:
                        await scroll_locator.scroll_into_view_if_needed(timeout=5000)  
                        await page.wait_for_timeout(2000)  

                        script_locator = page.locator(f"xpath={script_xpath}")
                        if await script_locator.count() > 0:
                            script_content = await script_locator.inner_html()
                            elements_count += 1
                            found_element = True

                            # Write to CSV file immediately
                            with open(file_path, mode='a', newline='', encoding='utf-8') as file:
                                writer = csv.writer(file)
                                writer.writerow([i, locality, script_content])
                                file.flush()  
                            break  
                        else:
                            await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                            await page.wait_for_timeout(5000)
                            break  
                except Exception as e:
                    if "Element is not attached to the DOM" in str(e):
                        await page.wait_for_timeout(1000)  
                    else:
                        raise e  

            if not found_element:
                failed_attempts += 1
                if failed_attempts >= max_failed_attempts:
                    break
            else:
                failed_attempts = 0  

        except Exception as e:
            continue

# Get the maximum number of elements
async def get_max_elements(page):
    try:
        result_locator = page.locator("xpath=/html/body/div/div/div/div[2]/div[4]/div/div/div[1]/div[3]/div")
        result_text = await result_locator.inner_text()
        match = re.search(r'(\d+) results', result_text)
        if match:
            return int(match.group(1))
        else:
            return 0
    except Exception as e:
        return 0

# Scraping function that runs concurrently, limited by the semaphore
async def scrape_locality_bedroom(page, locality, bedroom):
    url = f"https://www.magicbricks.com/property-for-rent/residential-real-estate?bedroom={bedroom}&Locality={locality}&cityName=New-Delhi"
    await page.goto(url)
    
    max_elements = await get_max_elements(page)
    
    await extract_elements(page, max_elements, locality)

async def main_scraping():
    bedroom_number_list = ["1", "2", "3", "4", "5", "%3E5"]
    locality_list = ['Chandni-Chowk', 'Civil-Lines', 'Daryaganj', 'Dariba-Kalan', 'Karol-Bagh', 'Old-Delhi']  # Sample localities

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)

        tasks = []
        with open(file_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['Index', 'Locality', 'Script Content'])

        # Loop through localities and bedrooms, and create tasks for parallel execution
        for locality in locality_list:
            for bedroom in bedroom_number_list:
                await semaphore.acquire()  # Limit the number of concurrent tasks
                page = await browser.new_page()
                
                task = asyncio.create_task(scrape_locality_bedroom(page, locality, bedroom))
                tasks.append(task)

                # Release semaphore after the task is done
                task.add_done_callback(lambda t: semaphore.release())

        # Wait for all tasks to complete
        await asyncio.gather(*tasks)

        await browser.close()

# Normalize the JSON content in the "Script Content" column
def normalize_json(file_path, output_file):
    df = pd.read_csv(file_path)

    df['Script Content'] = df['Script Content'].apply(json.loads)

    df_normalized = json_normalize(df['Script Content'])

    df_final = pd.concat([df.drop(columns=['Script Content']), df_normalized], axis=1)

    df_final.to_csv(output_file, index=False)

    return df_final

# Integrate all steps
async def main():
    # Step 1: Scrape data using Playwright
    await main_scraping()

    # Step 2: Normalize the JSON data
    df_normalized = normalize_json(file_path, output_normalized_file)

    # Step 3: Extract rent and price information using Selenium and save the final data
    extract_rent_and_save(df_normalized, output_final_file)

import asyncio

if __name__ == "__main__":
    asyncio.run(main())
