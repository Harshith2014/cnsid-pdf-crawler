import requests
from bs4 import BeautifulSoup

# Direct ScienceDirect URL using the known PII
url = "https://www.sciencedirect.com/science/article/pii/S0021925820829378"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/"
}

try:
    print(f"Fetching {url}...")
    response = requests.get(url, headers=headers, allow_redirects=True, timeout=10)
    print(f"Final URL: {response.url}")
    print(f"Status Code: {response.status_code}")
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Check for citation_pdf_url
    meta_pdf = soup.find("meta", {"name": "citation_pdf_url"})
    if meta_pdf:
        print(f"Found citation_pdf_url: {meta_pdf['content']}")
    else:
        print("No citation_pdf_url meta tag found.")
        # Debug: print title to see if we are blocked
        print(f"Page Title: {soup.title.string if soup.title else 'No Title'}")
        
    # Check for other PDF links
    pdf_link = soup.find("a", string="View PDF")
    if pdf_link:
        print(f"Found 'View PDF' link: {pdf_link.get('href')}")

except Exception as e:
    print(f"Error: {e}")
