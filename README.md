# PDF Crawler with Selenium & Unpaywall

A robust Python crawler designed to download PDF documents from a CSV list of CNSIDs/DOIs at scale. It leverages the [Unpaywall API](https://unpaywall.org/) to find legal Open Access links and uses Selenium (headless Chrome) to download files, mimicking a real browser to bypass common bot detections (403 Forbidden).

## Features

- **Consolidated Dependencies**: Uses `selenium` and `webdriver-manager` for browser automation.
- **Bot Bypass**: Handles redirects and javascript-heavy pages (ScienceDirect, etc.) that block simple requests.
- **Smart Sharding**: Automatically saves files into `shard_0/`, `shard_1/` subdirectories (1000 files/folder) to prevent filesystem issues.
- **Auto-Migration**: Detects files in the root folder or wrong location and moves them to the correct shard automatically.
- **Resumable**: Skips already downloaded files.
- **Robustness**: Handles network interruptions, timeouts, and locked files gracefully.

## Prerequisites

- Python 3.8+
- Google Chrome browser installed.

## Installation

1. Clone the repository (or download the source).
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

1. **Configure Input**:
   Update the `INPUT_CSV` path in `crawler_selenium.py` to point to your CSV file. The CSV must have `cnsid` and `doi` (or `pdf_url`) columns.

2. **Run the Crawler**:
   ```bash
   python crawler_selenium.py
   ```

3. **Output**:
   PDFs will be saved in `pdfs_final/shard_X/`.
   Logs are saved to `download_log.csv`.

## Configuration

Edit `crawler_selenium.py` to adjust:
- `INPUT_CSV`: Path to your input data.
- `OUTPUT_DIR`: Directory for downloads (default: `pdfs_final`).
- `SHARD_SIZE`: Files per folder (default: 1000).
- `LIMIT_ROWS`: Set to `None` for full run or an integer for testing.
