## How the scraper works:
1. Read Domains: The script reads a list of company domains from a .parquet file.
2. Visit & Search: For each domain, it visits the website like a browser, then scans the page's HTML code for common clues that point to a logo URL.
3. Handle Errors & Blocks: It automatically retries if a site is temporarily down and rotates its identity to avoid being blocked.
4. Save Successes: If a logo is found, it saves the domain and the logo's URL to a .csv file, skipping all the failures.
   
## How the cluster algorithm works:
1. Creates a Visual "Fingerprint": For each logo, the algorithm downloads it and shrinks it down to a tiny 8x8 pixel grid. It does this twice: once in color and once in black and white. This process creates a set of numbers that acts as a unique fingerprint for the logo's shape and color scheme.
2. Builds a Similarity Network: The algorithm then compares every logo against every other logo. If two logos are very similar—meaning their fingerprints match closely in both shape and color it draws a connecting line between them.
3. Identifies the Groups: Finally, the clusters are simply the groups of logos that are connected by these lines. Every group, including single logos that didn't match anything else, is then written into the output file as its own separate cluster.
