import csv
import os
import time
import requests
import shutil
import concurrent.futures
import threading
import numpy as np
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# CONFIGURATION
INPUT_CSV = r'C:\Users\M Harshith\Downloads\jbc.csv'
OUTPUT_DIR = os.path.abspath('pdfs_final')
TEMP_DIR = os.path.abspath('pdfs_temp')
LOG_FILE = 'download_log.csv'
UNPAYWALL_EMAIL = 'harshith.research@gmail.com'
MAX_RETRIES = 3
WAIT_TIME_DOWNLOAD = 15  # seconds to wait for download
LIMIT_ROWS = None # Use None for full run, or int for testing
SHARD_SIZE = 1000 # Number of files per shard folder
MAX_WORKERS = 3   # Number of parallel browsers

# Global lock for CSV writing
csv_lock = threading.Lock()

# Ensure directories exist
for directory in [OUTPUT_DIR, TEMP_DIR]:
    if not os.path.exists(directory):
        os.makedirs(directory)

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
        # print(f"Error checking Unpaywall for {doi}: {e}")
        pass
    return None

def setup_driver(download_dir):
    """Configure and launch Chrome driver with specific download dir."""
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless") # Comment out to see browser for debugging
    options.add_experimental_option("prefs", {
      "download.default_directory": download_dir,
      "download.prompt_for_download": False,
      "download.directory_upgrade": True,
      "plugins.always_open_pdf_externally": True
    })
    # Try to install driver using manager, or fallback to system driver
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
    except Exception as e:
        print(f"Driver Manager failed: {e}. Trying system driver...", flush=True)
        driver = webdriver.Chrome(options=options)
    
    return driver

def process_batch(df_chunk, worker_id, csv_writer):
    """Process a chunk of rows with a dedicated browser instance."""
    print(f"Worker {worker_id} started processing {len(df_chunk)} rows.", flush=True)
    
    # Create unique temp dir for this worker
    worker_temp_dir = os.path.join(TEMP_DIR, f"worker_{worker_id}")
    if not os.path.exists(worker_temp_dir):
        os.makedirs(worker_temp_dir)
        
    driver = setup_driver(worker_temp_dir)
    
    try:
        for index, row in df_chunk.iterrows():
            cnsid = row['cnsid']
            doi = row.get('doi')
            original_pdf_url = row.get('pdf_url')
            
            # Determine shard directory
            shard_index = index // SHARD_SIZE
            shard_dir = os.path.join(OUTPUT_DIR, f"shard_{shard_index}")
            if not os.path.exists(shard_dir):
                try:
                    os.makedirs(shard_dir, exist_ok=True)
                except:
                    pass # Race condition safe

            # Define final path within shard
            final_path = os.path.join(shard_dir, f"{cnsid}.pdf")
            
            # Check if file exists in shard
            if os.path.exists(final_path):
                print(f"[Worker {worker_id}] Skipping {cnsid}: Already exists.", flush=True)
                with csv_lock:
                    csv_writer.writerow([cnsid, doi, original_pdf_url, '', 'Skipped', 'File exists'])
                continue

            # Check if file exists in root (migration logic)
            root_path = os.path.join(OUTPUT_DIR, f"{cnsid}.pdf")
            if os.path.exists(root_path):
                try:
                    shutil.move(root_path, final_path)
                    print(f"[Worker {worker_id}] Migrated {cnsid} to {shard_dir}", flush=True)
                    with csv_lock:
                        csv_writer.writerow([cnsid, doi, original_pdf_url, '', 'Skipped', 'Migrated to shard'])
                    continue
                except Exception as e:
                    print(f"[Worker {worker_id}] Error migrating {cnsid}: {e}", flush=True)

            print(f"[Worker {worker_id}] Processing {cnsid}...", flush=True)
            
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
                print(f"[Worker {worker_id}]   No valid URL found for {cnsid}", flush=True)
                with csv_lock:
                    csv_writer.writerow([cnsid, doi, original_pdf_url, '', 'Failed', 'No URL'])
                continue

            print(f"[Worker {worker_id}]   Attempting download from ({source}): {download_url}", flush=True)
            
            try:
                # Clear worker temp dir before download
                for f in os.listdir(worker_temp_dir):
                    file_path = os.path.join(worker_temp_dir, f)
                    try:
                        if os.path.isfile(file_path):
                            os.unlink(file_path)
                    except Exception as e:
                        pass

                driver.get(download_url)
                
                # Wait for download
                downloaded_file = None
                start_time = time.time()
                while time.time() - start_time < WAIT_TIME_DOWNLOAD:
                    files = [f for f in os.listdir(worker_temp_dir) if not f.endswith('.crdownload') and not f.endswith('.tmp')]
                    if files:
                        downloaded_file = os.path.join(worker_temp_dir, files[0])
                        time.sleep(1) 
                        break
                    time.sleep(1)

                if downloaded_file and os.path.exists(downloaded_file):
                    # Rename and Move
                    shutil.move(downloaded_file, final_path)
                    print(f"[Worker {worker_id}]   Success: Saved to {final_path}", flush=True)
                    with csv_lock:
                        csv_writer.writerow([cnsid, doi, original_pdf_url, download_url, 'Success', ''])
                else:
                    print(f"[Worker {worker_id}]   Failed: Timeout or no file.", flush=True)
                    with csv_lock:
                        csv_writer.writerow([cnsid, doi, original_pdf_url, download_url, 'Failed', 'Download timeout'])

            except Exception as e:
                print(f"[Worker {worker_id}]   Error: {e}", flush=True)
                with csv_lock:
                    csv_writer.writerow([cnsid, doi, original_pdf_url, download_url, 'Failed', str(e)])
                
                # Re-initialize driver if needed
                try:
                    driver.title
                except:
                    print(f"[Worker {worker_id}]   Restarting driver...", flush=True)
                    try:
                        driver.quit()
                    except:
                        pass
                    driver = setup_driver(worker_temp_dir)
                    
    finally:
        driver.quit()
        # Cleanup worker temp dir
        try:
            shutil.rmtree(worker_temp_dir)
        except:
            pass

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
    log_file = open(LOG_FILE, 'a', newline='', encoding='utf-8') # Append mode
    csv_writer = csv.writer(log_file)
    # Write header only if file is empty
    if os.stat(LOG_FILE).st_size == 0:
        csv_writer.writerow(['cnsid', 'doi', 'pdf_url', 'download_url', 'status', 'error_msg'])

    # Determine chunks
    chunks = np.array_split(df, MAX_WORKERS)
    
    print(f"Starting {MAX_WORKERS} workers...", flush=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for i, chunk in enumerate(chunks):
            if not chunk.empty:
                futures.append(executor.submit(process_batch, chunk, i, csv_writer))
        
        concurrent.futures.wait(futures)

    log_file.close()
    print("Crawler completed.", flush=True)

if __name__ == "__main__":
    main()
