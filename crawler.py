import csv
import asyncio
import aiohttp
import os
import re
from urllib.parse import urlparse, parse_qs, unquote

INPUT_CSV = r'C:\Users\M Harshith\Downloads\jbc.csv'
OUTPUT_DIR = 'pdfs'
ERROR_LOG = 'download_errors.csv'
CONCURRENCY_LIMIT = 5
MAX_RETRIES = 3
LIMIT_ROWS = 5 # Limit to 5 for initial test

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

async def get_pii_from_url(session, url):
    try:
        async with session.get(url, allow_redirects=True) as response:
            final_url = str(response.url)
            text = await response.text()
            
            # Check for ScienceDirect/Elsevier PII in URL
            # Standard pattern: /science/article/pii/S...
            pii_match = re.search(r'/pii/(S[0-9A-Z]+)', final_url)
            if pii_match:
                return pii_match.group(1)

            # Check for meta-refresh redirect (common with DOIs pointing to ScienceDirect)
            # <meta HTTP-EQUIV="REFRESH" content="2; url='...'"
            meta_refresh = re.search(r'content=["\']\d+;\s*url=[\'"](.*?)[\'"]', text, re.IGNORECASE)
            if meta_refresh:
                redirect_url = meta_refresh.group(1)
                # The redirect URL might be URL-encoded or relative
                if not redirect_url.startswith('http'):
                     # It might be relative, but usually these are absolute or within the same domain context
                     # Let's try to parse it for PII directly if possible
                     pass
                
                # Check for PII in the redirect URL
                pii_match_redirect = re.search(r'/pii/(S[0-9A-Z]+)', redirect_url)
                if pii_match_redirect:
                    return pii_match_redirect.group(1)
                
                # Sometimes the redirect URL itself needs to be followed, but often the PII is in the 'Redirect' param
                # e.g., url='/retrieve/...?Redirect=...pii%2FS0021925820829378...'
                decoded_redirect = unquote(redirect_url)
                pii_match_decoded = re.search(r'/pii/(S[0-9A-Z]+)', decoded_redirect)
                if pii_match_decoded:
                    return pii_match_decoded.group(1)

            print(f"PII not found for {url}")
            return None
    except Exception as e:
        print(f"Error resolving PII for {url}: {e}")
        return None

async def download_pdf(session, cnsid, url, semaphore):
    async with semaphore:
        pdf_path = os.path.join(OUTPUT_DIR, f"{cnsid}.pdf")
        if os.path.exists(pdf_path):
            print(f"Skipping {cnsid}, already exists.")
            return "Skipped"

        print(f"Resolving PII for {cnsid} ({url})...")
        pii = await get_pii_from_url(session, url)
        if not pii:
            print(f"Failed to resolve PII for {cnsid}")
            return f"Failed to resolve PII for {url}"

        # Construct ScienceDirect PDF URL
        pdf_url = f"https://www.sciencedirect.com/science/article/pii/{pii}/pdfft?isDTMRedir=true&download=true"
        
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            async with session.get(pdf_url, headers=headers) as response:
                if response.status == 200:
                    with open(pdf_path, 'wb') as f:
                        while True:
                            chunk = await response.content.read(1024)
                            if not chunk:
                                break
                            f.write(chunk)
                    print(f"Downloaded {cnsid} to {pdf_path}", flush=True)
                    return "Success"
                else:
                    print(f"Failed download {cnsid}: Status {response.status}", flush=True)
                    return f"Failed to download PDF (Status {response.status})"
        except Exception as e:
            return f"Error downloading PDF: {e}"

async def main():
    # Read CSV with latin1 to handle special chars if any
    try:
        import pandas as pd
        df = pd.read_csv(INPUT_CSV, encoding='latin1')
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    async with aiohttp.ClientSession() as session:
        tasks = []
        # Limit processing
        df_subset = df.head(LIMIT_ROWS)
        print(f"Processing {len(df_subset)} rows...")
        
        for index, row in df_subset.iterrows():
            cnsid = row['cnsid']
            # Prefer DOI if available and looks like a URL, else pdf_url
            url = row['pdf_url']
            if 'doi' in row and pd.notna(row['doi']):
                 doi = str(row['doi']).strip()
                 if doi.startswith('10.'):
                     url = f"https://doi.org/{doi}"
                 elif doi.startswith('http'):
                     url = doi

            if not url or pd.isna(url):
                print(f"No URL for {cnsid}")
                continue

            tasks.append(download_pdf(session, cnsid, url, semaphore))

        results = await asyncio.gather(*tasks)

        # Log errors
        with open(ERROR_LOG, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['cnsid', 'status'])
            for i, result in enumerate(results):
                if result != "Success" and result != "Skipped":
                    writer.writerow([df.iloc[i]['cnsid'], result])

if __name__ == "__main__":
    asyncio.run(main())
