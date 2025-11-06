Marian University Broken-Link Checker
====================================

This program scans the Marian University website for broken links.  
It can run in mock mode (offline test) or real mode (using the live sitemap).

------------------------------------------------------------
HOW TO RUN THE PROGRAM
------------------------------------------------------------

1) Create a Virtual Environment
--------------------------------
Windows:
    python -m venv .venv
    .venv\Scripts\activate

macOS / Linux:
    python3 -m venv .venv
    source .venv/bin/activate


2) Install Dependencies
------------------------
    pip install -r requirements.txt


3) Configure the Program
-------------------------
Open the file "config.yaml" and adjust the settings as needed.  
You can choose between two modes:

Mock Mode (offline test with local files):
    mock_mode: true
    mock:
      sitemap_path: "data/mock/sitemap.xml"
      sample_page_url: "https://www.marian.edu/mock"
      sample_page_path: "data/mock/page.html"

Real Mode (scan the live website):
    mock_mode: false
    sitemap_url: "https://www.marian.edu/sitemap.xml"

Optional setting for Excel compatibility:
    csv_delimiter: ";"


4) Run the Program
-------------------
In your terminal (inside the project folder):
    python main.py


5) Check the Results
---------------------
After running, two CSV files will be created:

    data/urls_initial.csv   →  All URLs extracted from the sitemap
    reports/links_sample.csv → Extracted internal links from one page

Open the CSV files in Excel to view the results.

During execution, the program also prints a quick performance metric:
    [fetch] sanity metric: 0.25 ms / request


Example Output (Mock Mode)
--------------------------
[sitemap] wrote 5 URLs -> data/urls_initial.csv
[fetch] sanity metric: 0.21 ms / request
[parse] internal=4 external=1
[report] wrote internal links -> reports/links_sample.csv


Notes
------
- Use mock mode first to safely test the crawler.
- Switch to real mode to scan the full Marian sitemap (≈1,500 pages).
- The program automatically removes duplicates and filters internal links.
- You can change the output delimiter (csv_delimiter) in config.yaml for your Excel version.


Author: Jonas Schaefer  
Marian University — Operating Systems Independent Study (Fall 2025)
