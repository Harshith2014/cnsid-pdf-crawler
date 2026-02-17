import csv
import os
import time
import requests
import shutil
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd

# CONFIGURATION
INPUT_CSV = r'C:\Users\M Harshith\Downloads\jbc.csv'
OUTPUT_DIR = os.path.abspath('pdfs_final')
TEMP_DIR = os.path.abspath('pdfs_temp')
LOG_FILE = 'download_log.csv'
UNPAYWALL_EMAIL = 'harshith.research@gmail.com'
MAX_RETRIES = 3
WAIT_TIME_DOWNLOAD = 15  # seconds to wait for download
WAIT_TIME_DOWNLOAD = 15  # seconds to wait for download
LIMIT_ROWS = None # Use None for full run
SHARD_SIZE = 1000 # Number of files per shard folder

# Ensure directories exist
for directory in [OUTPUT_DIR, TEMP_DIR]:
    if not os.path.exists(directory):
        os.makedirs(directory)

# Clear temp directory
for file in os.listdir(TEMP_DIR):
    file_path = os.path.join(TEMP_DIR, file)
    try:
        if os.path.isfile(file_path):
            os.unlink(file_path)
    except Exception as e:
        print(f"Error clearing temp file {file_path}: {e}")

def get_unpaywall_url(doi):
    """Retrieve the best OA PDF URL from Unpaywall API."""
    if not doi:
        return None
        
    url = f"https://api.unpaywall.org/v2/{doi}?email={UNPAYWALL_EMAIL}"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            best_oa = data.get('best_oa_location')
            if best_oa:
                return best_oa.get('url_for_pdf')
    except Exception as e:
        print(f"Error checking Unpaywall for {doi}: {e}")
    return None

def setup_driver():
    """Configure and launch Chrome driver."""
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless") # Comment out to see browser for debugging
    options.add_experimental_option("prefs", {
      "download.default_directory": TEMP_DIR,
      "download.prompt_for_download": False,
      "download.directory_upgrade": True,
      "plugins.always_open_pdf_externally": True
    })
    # Try to install driver using manager, or fallback to system driver
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
    except Exception as e:
        print(f"Driver Manager failed: {e}. Trying system driver...")
        driver = webdriver.Chrome(options=options)
    
    return driver

def main():
    # Read CSV
    try:
        df = pd.read_csv(INPUT_CSV, encoding='latin1')
        if LIMIT_ROWS:
            df = df.head(LIMIT_ROWS)
            print(f"Running in LIMITED mode: Processing first {LIMIT_ROWS} rows.")
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    # Setup Logging
    log_file = open(LOG_FILE, 'w', newline='', encoding='utf-8')
    csv_writer = csv.writer(log_file)
    csv_writer.writerow(['cnsid', 'doi', 'pdf_url', 'download_url', 'status', 'error_msg'])

    driver = setup_driver()

    try:
        for index, row in df.iterrows():
            cnsid = row['cnsid']
            doi = row.get('doi')
            original_pdf_url = row.get('pdf_url')
            
            # Determine shard directory
            shard_index = index // SHARD_SIZE
            shard_dir = os.path.join(OUTPUT_DIR, f"shard_{shard_index}")
            if not os.path.exists(shard_dir):
                os.makedirs(shard_dir)

            # Define final path within shard
            final_path = os.path.join(shard_dir, f"{cnsid}.pdf")
            
            # Check if file exists in shard
            if os.path.exists(final_path):
                print(f"Skipping {cnsid}: Already exists in {shard_dir}.")
                csv_writer.writerow([cnsid, doi, original_pdf_url, '', 'Skipped', 'File exists'])
                continue

            # Check if file exists in root (migration logic)
            root_path = os.path.join(OUTPUT_DIR, f"{cnsid}.pdf")
            if os.path.exists(root_path):
                try:
                    shutil.move(root_path, final_path)
                    print(f"Migrated {cnsid} to {shard_dir}")
                    csv_writer.writerow([cnsid, doi, original_pdf_url, '', 'Skipped', 'Migrated to shard'])
                    continue
                except Exception as e:
                    print(f"Error migrating {cnsid}: {e}")

            print(f"Processing {cnsid} ({index})...")
            
            # 1. Try Unpaywall
            download_url = get_unpaywall_url(doi)
            source = "Unpaywall"
            
            # 2. Fallback to CSV URL or DOI construction
            if not download_url:
                if original_pdf_url and pd.notna(original_pdf_url):
                    download_url = original_pdf_url
                    source = "CSV"
                elif doi and pd.notna(doi):
                     if str(doi).startswith('10.'):
                         download_url = f"https://doi.org/{str(doi)}"
                         source = "DOI"

            if not download_url:
                print(f"  No valid URL found for {cnsid}")
                csv_writer.writerow([cnsid, doi, original_pdf_url, '', 'Failed', 'No URL'])
                continue

            print(f"  Attempting download from ({source}): {download_url}")
            
            try:
                # Clear temp dir before download to ensure we get the right file
                # Use robust error handling
                for f in os.listdir(TEMP_DIR):
                    file_path = os.path.join(TEMP_DIR, f)
                    try:
                        if os.path.isfile(file_path):
                            os.unlink(file_path)
                    except Exception as e:
                        print(f"  Warning: Could not clear temp file {f}: {e}")

                driver.get(download_url)
                
                # Wait for download
                # Intelligent wait: check for file appearance
                downloaded_file = None
                start_time = time.time()
                while time.time() - start_time < WAIT_TIME_DOWNLOAD:
                    files = [f for f in os.listdir(TEMP_DIR) if not f.endswith('.crdownload') and not f.endswith('.tmp')]
                    if files:
                        downloaded_file = os.path.join(TEMP_DIR, files[0])
                        # Wait a bit more to ensure write completion if needed, though .crdownload check helps
                        time.sleep(1) 
                        break
                    time.sleep(1)

                if downloaded_file and os.path.exists(downloaded_file):
                    # Rename and Move
                    shutil.move(downloaded_file, final_path)
                    print(f"  Success: Saved to {final_path}")
                    csv_writer.writerow([cnsid, doi, original_pdf_url, download_url, 'Success', ''])
                else:
                    print(f"  Failed: Timeout or no file in temp dir.")
                    csv_writer.writerow([cnsid, doi, original_pdf_url, download_url, 'Failed', 'Download timeout'])

            except Exception as e:
                print(f"  Error: {e}")
                csv_writer.writerow([cnsid, doi, original_pdf_url, download_url, 'Failed', str(e)])
                
                # Re-initialize driver if it crashed
                try:
                    driver.title
                except:
                    print("  Restarting driver...")
                    driver.quit()
                    driver = setup_driver()

    finally:
        driver.quit()
        log_file.close()
        print("Crawler completed.")

if __name__ == "__main__":
    main()
