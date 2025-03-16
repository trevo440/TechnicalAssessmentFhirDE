'''
CMS Download Manager
- API Documentation: https://data.cms.gov/provider-data/docs
- Dataset (ALL): https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items

This script will grab the download link from all {"theme": ["Hospitals", *]} datasets
And clean up the column names.

Process:
    > Get all datasets with "Hospitals" in the theme
    > Get all download URLs from the dataset
    > Filter out datasets that have already been downloaded
    > Save the state of the NEW download dataset URLs
    > Download in parallel
    > Execute Daily
'''

# IMPORTS
from resources import ResourceQuery
from metadata import StateManager
import requests
import io
import pandas as pd
import threading
import pathlib

# Set to 0 for all, as a warning it downloads ALOT of data.
max_downloads = 2
reset_state = True

# --------------------------------------------
# 1. GET ALL DOWNLOAD URLS
# --------------------------------------------
resource_query_url = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
state_manager = StateManager('cms_dataset_items_state.json')

RS = ResourceQuery(
    resource_query_url = resource_query_url, 
    state_manager = state_manager,
)
download_urls = RS.get_download_dataset_urls()

# --------------------------------------------
# 2. THREADED DOWNLOAD + PROCESS (PARALLEL)
# --------------------------------------------

dir = pathlib.Path("downloads")
dir.mkdir(parents=True, exist_ok=True)

def threaded_download_and_process(url: str):
    """
    :param: url: str - URL to download
    :description: Function to download the file from the given URL
    """
    response = requests.get(url)
    response.raise_for_status()
    
    file_like = io.BytesIO(response.content)
    df = pd.read_csv(file_like)
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]
    df.columns = df.columns.str.replace(r'[^a-zA-Z0-9\s]', '_', regex=True)
    
    df.to_csv("downloads/" + url.split("/")[-1], index=False)
    print(f"Downloaded and processed: {url}")

threads = []

count_downloads = 0
for url in download_urls:
    if max_downloads:
        if count_downloads >= max_downloads:
            break
    print(f"Downloading: {url}")
    thread = threading.Thread(target=threaded_download_and_process, args=(url,))
    threads.append(thread)
    thread.start()
    count_downloads += 1

for thread in threads:
    thread.join()

# --------------------------------------------
# RESET STATE FOR TESTS
# --------------------------------------------
if reset_state:
    print("Resetting state...")
    state_manager.clear_state()

# --------------------------------------------
# @ NOTE: Run twice if commenting out for
# The state to be created, then only new items
# returned

