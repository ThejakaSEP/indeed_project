import glob
import os
import pandas as pd
import schedule
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from selenium.webdriver.common.action_chains import ActionChains
from datetime import datetime


def run_filter(driver):
    # Locate the job title and location fields and enter the values
    job_title_input = driver.find_elements(By.ID, 'text-input-what')[0]
    location_input = driver.find_elements(By.ID, 'text-input-where')[0]

    job_title_input.send_keys("Data Analyst")

    location_input.send_keys(Keys.COMMAND + "a")
    location_input.send_keys(Keys.DELETE)
    location_input.send_keys("toronto")

    location_input.send_keys(Keys.RETURN)  # Simulates hitting the enter key

    time.sleep(1)

    # Filter by Date Posted
    date_posted_dropdown = driver.find_elements(By.ID, "filter-dateposted")[0]  # Adjust the element selector as needed
    date_posted_dropdown.click()
    time.sleep(1)

    link = driver.find_element(By.LINK_TEXT, "Last 24 hours")
    link.click()
    time.sleep(1)

    # Press enter to close the popup
    actions = ActionChains(driver)
    actions.send_keys(Keys.ENTER)
    actions.perform()

    # Filter by radius
    date_posted_dropdown = driver.find_elements(By.ID, "filter-radius")[0]  # Adjust the element selector as needed
    date_posted_dropdown.click()
    time.sleep(1)

    link = driver.find_element(By.LINK_TEXT, "Within 100 kilometres")
    link.click()
    time.sleep(1)

def job_search(driver):

    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')

    job_list = soup.find('ul', class_='css-zu9cdh eu4oa1w0')

    job_title_list = []
    job_employer_list = []

    for li in job_list.find_all('li', recursive=False):
        try:
            job_title_tag = li.find('h2', class_='jobTitle').find(['span', 'a'])
            job_title = job_title_tag.get('title') if job_title_tag.name == 'span' and job_title_tag.get(
                'title') else job_title_tag.get_text(strip=True)
            job_title_list.append(job_title)
            print(job_title)

            employer_tag = li.find('span', {'data-testid': 'company-name'})
            employer_name = employer_tag.get_text(strip=True) if employer_tag else "Employer not found"
            job_employer_list.append(employer_name)
            print(employer_name)

        except AttributeError:
            continue

    return job_title_list, job_employer_list

def load_previous_data(filepath):
    try:
        return pd.read_excel(filepath)
    except FileNotFoundError:
        return pd.DataFrame(columns=["Job Title", "Employer", "Timestamp"])

def save_to_excel(df, base_filepath):
    current_time = datetime.now()
    timestamp_str = current_time.strftime("%Y-%m-%d_%H%M")
    filename = f"{base_filepath}_{timestamp_str}.xlsx"
    df.to_excel(filename, index=False)

def update_dataframe(existing_data, new_titles, new_employers):
    timestamp = datetime.now()
    # Create a DataFrame for the new data
    new_data = pd.DataFrame({
        "Job Title": new_titles,
        "Employer": new_employers
    })

    # Check for new entries and update the timestamp accordingly
    for index, row in new_data.iterrows():
        if not ((existing_data['Job Title'] == row['Job Title']) & (existing_data['Employer'] == row['Employer'])).any():
            # It's a new entry, set the current timestamp
            row['Timestamp'] = timestamp
            # Append this new entry to the existing DataFrame
            existing_data = pd.concat([existing_data, pd.DataFrame([row])], ignore_index=True)
        # If it's an existing entry, we do nothing, so the original timestamp remains

    return existing_data

def load_previous_data(file_pattern):
    # List all files in the current directory that match the file pattern
    list_of_files = glob.glob(file_pattern)
    if not list_of_files:  # If the list is empty, no files were found
        return pd.DataFrame(columns=["Job Title", "Employer", "Timestamp"])

    # Find the most recent file by sorting the list of files based on their modification time
    latest_file = max(list_of_files, key=os.path.getmtime)
    # Load and return the data from the most recent file
    return pd.read_excel(latest_file, index_col=None)


def run_job_search_every_hour():
    filepath = 'job_data'
    filepath_with_extension = f"{filepath}.xlsx"

    base_file_pattern = 'job_data*.xlsx'

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.maximize_window()
    driver.get("https://www.indeed.com")
    time.sleep(2)

    run_filter(driver)

    # Load the previous data if it exists
    existing_data = load_previous_data(base_file_pattern)

    # Perform job search and get the new data
    job_titles, employers = job_search(driver)

    # Update the DataFrame with new entries
    updated_data = update_dataframe(existing_data, job_titles, employers)

    # Save updated data to Excel with a timestamp in the filename if new data has been added
    if not existing_data.equals(updated_data):
        save_to_excel(updated_data, filepath)

    # Close the driver after the operation is complete
    driver.quit()

# Schedule the job to run every minute
schedule.every(1).minutes.do(run_job_search_every_hour)

# Run the scheduled job in an infinite loop
while True:
    schedule.run_pending()
    time.sleep(1)
