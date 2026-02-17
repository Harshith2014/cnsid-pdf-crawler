from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import os

pdf_url = "https://www.jbc.org/article/S0021925820829378/pdf"
download_dir = os.path.abspath("pdfs")

if not os.path.exists(download_dir):
    os.makedirs(download_dir)

options = webdriver.ChromeOptions()
# options.add_argument("--headless") # Comment out for debugging
options.add_experimental_option("prefs", {
  "download.default_directory": download_dir,
  "download.prompt_for_download": False,
  "download.directory_upgrade": True,
  "plugins.always_open_pdf_externally": True
})

print(f"Setting up Chrome driver...")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

try:
    print(f"Navigating to {pdf_url}...")
    driver.get(pdf_url)
    
    # Wait for download to start/complete (simple sleep for now)
    print("Waiting for download...")
    time.sleep(10)
    
    # Check if any file appeared in download_dir
    files = os.listdir(download_dir)
    if files:
        print(f"Downloaded files: {files}")
    else:
        print("No files downloaded.")
        
finally:
    driver.quit()
