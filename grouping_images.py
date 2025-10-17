import pandas as pd
import requests
from PIL import Image, UnidentifiedImageError
import numpy as np
import networkx as nx
import io
import concurrent.futures
import sys
from datetime import datetime
import cairosvg

LOGO_URL_COLUMN = 'logo_url'
DOMAIN_NAME_COLUMN = 'domain_name'
OUTPUT_FILENAME = 'clustere_logo.txt'
GRAY_VECTOR_THRESHOLD = 0.05
COLOR_VECTOR_THRESHOLD = 0.05

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
    'Accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.google.com/'
}

def get_image_fingerprints(image_url):
    if not isinstance(image_url, str) or not image_url.startswith('http'):
        return None, "URL invalid"
    try:
        response = requests.get(image_url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        image_data = response.content
        
        content_type = response.headers.get('Content-Type', '')
        if 'svg' in content_type or image_url.endswith('.svg'):
            png_data = cairosvg.svg2png(bytestring=image_data)
            image = Image.open(io.BytesIO(png_data))
        else:
            image = Image.open(io.BytesIO(image_data))
        
        if image.mode in ('RGBA', 'P', 'LA'):
            background = Image.new('RGB', image.size, (255, 255, 255))
            if 'A' in image.mode:
                background.paste(image, mask=image.split()[-1])
            else:
                background.paste(image)
            image = background
        elif image.mode != 'RGB':
            image = image.convert('RGB')
        
        fingerprints = {
            'gray_vector': np.array(image.convert('L').resize((8, 8), Image.Resampling.LANCZOS), dtype=np.float32).flatten(),
            'color_vector': np.array(image.resize((8, 8), Image.Resampling.LANCZOS), dtype=np.float32).flatten()
        }
        return fingerprints, None
    except (requests.RequestException, UnidentifiedImageError, Exception) as e:
        return None, str(e)

def calculate_normalized_distance(vec1, vec2):
    diff = vec1 - vec2
    distance = np.sqrt(np.sum(diff**2))
    max_dist = np.sqrt(len(vec1) * (255**2))
    return distance / max_dist if max_dist > 0 else 0

def process_row(row_tuple):
    index, row = row_tuple
    domain = row[DOMAIN_NAME_COLUMN]
    url = row[LOGO_URL_COLUMN]
    print(f"Procesez: {domain}...")
    fingerprints, error = get_image_fingerprints(url)
    if error:
        print(f"  ->{domain}: {error}", file=sys.stderr)
        return None
    print(f"  -> Amprentă generată pentru {domain}")
    return {'domain': domain, 'fingerprints': fingerprints}

def main(csv_path):
    try:
        df = pd.read_csv(csv_path)
        if not all(col in df.columns for col in [LOGO_URL_COLUMN, DOMAIN_NAME_COLUMN]):
            print(f"Coloanele '{LOGO_URL_COLUMN}' și '{DOMAIN_NAME_COLUMN}' trebuie să existe in {csv_path}!", file=sys.stderr)
            return
    except FileNotFoundError:
        print(f"Fișierul {csv_path} nu a fost găsit!", file=sys.stderr)
        return

    df_valid = df.dropna(subset=[LOGO_URL_COLUMN, DOMAIN_NAME_COLUMN]).copy()
    
    processed_data = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(process_row, df_valid.iterrows())
        processed_data = [res for res in results if res is not None]

    if len(processed_data) < 2:
        print("\nNu au fost procesate suficiente imagini pentru a forma grupuri.")
        return

    print("\nConstruire graf...")
    G = nx.Graph()
    nodes = [item['domain'] for item in processed_data]
    G.add_nodes_from(nodes)

    for i in range(len(processed_data)):
        for j in range(i + 1, len(processed_data)):
            item1, item2 = processed_data[i], processed_data[j]
            fp1, fp2 = item1['fingerprints'], item2['fingerprints']

            dist_gray = calculate_normalized_distance(fp1['gray_vector'], fp2['gray_vector'])
            dist_color = calculate_normalized_distance(fp1['color_vector'], fp2['color_vector'])

            if dist_gray <= GRAY_VECTOR_THRESHOLD and dist_color <= COLOR_VECTOR_THRESHOLD:
                G.add_edge(item1['domain'], item2['domain'])
    
    clusters = list(nx.connected_components(G))
    
    print(f"\nProces finalizat. Scriere Rezultate '{OUTPUT_FILENAME}'...")
    
    with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as f:

        sorted_clusters = sorted(clusters, key=lambda c: (-len(c), sorted(list(c))[0]))
        
        cluster_num = 1
        for cluster_set in sorted_clusters:
            f.write(f"Cluster {cluster_num}:\n")
            for domain in sorted(list(cluster_set)):
                f.write(f"  - {domain}\n")
            f.write("\n")
            cluster_num += 1
                 
if __name__ == '__main__':
    csv_file_path = r'C:\Users\Florin\Documents\Veridion Assesment\logos_output.csv'
    main(csv_file_path)
