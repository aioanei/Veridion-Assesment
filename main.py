import pandas as pd
import requests
import time
import csv
import random
import socket
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
]

def scrape_logos_final(parquet_file_path: str, output_csv_path: str, domain_column: str = 'domain'):
    
    RETRY_STATUS_CODES = {500, 502, 503, 504, 429}

    MAX_WAIT_TIME = 5

    try:
        df = pd.read_parquet(parquet_file_path)
    except Exception as e:
        print(f"Error reading Parquet file: {e}")
        return

    with open(output_csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['domain_name', 'logo_url', 'status'])

        for domain in df[domain_column]:
            if not isinstance(domain, str) or not domain:
                continue
            
            domain = domain.strip()
            
            try:
                socket.gethostbyname(domain)
            except socket.gaierror:
                print(f"DNS lookup failed for '{domain}'. Skipping.")
                continue

            url = f"https://{domain}"
            status = 'Failed'
            found_logo_url = ''
            
            for attempt in range(2):
                try:
                    headers = {'User-Agent': random.choice(USER_AGENTS)}
                    
                    print(f"Attempt {attempt + 1}/2: Scraping {url}")
                    response = requests.get(url, headers=headers, timeout=1, verify=False)
                    
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.content, 'html.parser')
                        logo_url = (
                            soup.find('meta', property='og:image') or
                            soup.find('link', rel='apple-touch-icon') or
                            soup.find('link', rel='icon') or
                            soup.find('img', {'src': lambda x: 'logo' in str(x).lower()})
                        )
                        
                        if logo_url:
                            logo_src = logo_url.get('content') or logo_url.get('href') or logo_url.get('src')
                            found_logo_url = urljoin(url, logo_src)
                            status = 'Success'
                        else:
                            status = 'Not Found'
                        break

                    elif response.status_code in RETRY_STATUS_CODES:
                        status = f'Retryable Error (HTTP {response.status_code})'
                        retry_after = response.headers.get('Retry-After')
                        
                        if retry_after:
                            wait_time = int(retry_after)
                            if wait_time > MAX_WAIT_TIME:
                                print(f"   -> Server requested wait of {wait_time}s exceeds max of {MAX_WAIT_TIME}s. Skipping domain.")
                                status = f'Skipped (Wait time too long: {wait_time}s)'
                                break
                            else:
                                print(f"   -> Server requested a wait of {wait_time}s.")
                                time.sleep(wait_time)
                        else:
                            time.sleep(2 ** (attempt + 1))
                        continue

                    else:
                        status = f'Failed (HTTP {response.status_code})'
                        break

                except requests.exceptions.RequestException as e:
                    status = f'Failed (Error: {type(e).__name__})'
                    print(f"   -> A network error occurred for {url}: {e}")
                    time.sleep(2 ** (attempt + 1))

            if status == 'Success':
                writer.writerow([domain, found_logo_url, status])
                f.flush()
            
            print(f" -> Result for {domain}: {status}\n")

    print(f"\n Scraping complete. Results saved to '{output_csv_path}'")

if __name__ == "__main__":
    file_path = r'C:\Users\Florin\Documents\Veridion Assesment\logos.snappy.parquet'

    scrape_logos_final(
        parquet_file_path=file_path, 
        output_csv_path='logos_output.csv'
    )
