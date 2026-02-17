import requests
import json

doi = "10.1074/jbc.M302972200"
email = "harshith.research@gmail.com" # Using a more realistic placeholder email

url = f"https://api.unpaywall.org/v2/{doi}?email={email}"

try:
    print(f"Fetching {url}...")
    response = requests.get(url, timeout=10)
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"Title: {data.get('title')}")
        
        # Check for best OA location
        best_oa = data.get('best_oa_location')
        if best_oa:
            pdf_url = best_oa.get('url_for_pdf')
            print(f"Best OA URL: {pdf_url}")
            print(f"Version: {best_oa.get('version')}")
            
            # Try to fetch PDF
            try:
                print(f"Testing PDF download from {pdf_url}...")
                pdf_response = requests.get(pdf_url, timeout=20, stream=True, allow_redirects=True, headers={'User-Agent': 'Mozilla/5.0'})
                print(f"PDF Response Status: {pdf_response.status_code}")
                print(f"PDF Final URL: {pdf_response.url}")
                if pdf_response.status_code == 200 and 'application/pdf' in pdf_response.headers.get('Content-Type', ''):
                    print("PDF is downloadable!")
                else:
                    print(f"Failed to download PDF. Content-Type: {pdf_response.headers.get('Content-Type')}")
            except Exception as e:
                print(f"Error downloading PDF: {e}")
        else:
            print("No OA location found.")
            
        # Check all OA locations
        oa_locations = data.get('oa_locations', [])
        print(f"Total OA locations: {len(oa_locations)}")
        for loc in oa_locations:
            print(f"- {loc.get('url_for_pdf')} ({loc.get('version')})")
            
    else:
        print(f"Error: {response.text}")
        
except Exception as e:
    print(f"Exception: {e}")
