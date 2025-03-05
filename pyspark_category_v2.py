import json
import os
import requests
import urllib.parse
import pandas as pd
import argparse
import time
import random
from tqdm import tqdm  # Progress bar
from concurrent.futures import ThreadPoolExecutor, as_completed  # Multithreading

# Argument Parser for user inputs
parser = argparse.ArgumentParser(description="Fetch Wikipedia categories for multiple languages dynamically.")
input_file = "intro+link_final_fix.csv"
output_file = "final_categories_en_fix.csv"
parser.add_argument("--languages", type=str, required=True, help="Comma-separated list of languages (e.g., 'en,hi,fr')")
args = parser.parse_args()

# List of User-Agent headers to avoid bot detection
USER_AGENTS = [
    "WikipediaFetcher/1.0 (Random User)",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "WikipediaResearchBot/1.1 (+https://example.com/bot)",
    "Python-urllib/3.9",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
]

# Cache to avoid duplicate requests
CATEGORY_CACHE = {}

def get_wikipedia_categories(page_title, language):
    """Fetches Wikipedia categories for a given page in a specified language, handling HTTP 429 errors."""
    if not page_title or pd.isna(page_title) or not isinstance(page_title, str):
        return ""

    page_title = page_title.split("/")[-1]
    page_title = urllib.parse.unquote(page_title)  # Decode URL encoding
    url = f"https://{language}.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "prop": "categories",
        "cllimit": "max",  # Reduce batch size to avoid API overload
        "redirects": 1,
        "titles": page_title,
        "format": "json",
    }

    # Check cache
    cache_key = (page_title, language)
    if cache_key in CATEGORY_CACHE:
        return CATEGORY_CACHE[cache_key]

    retries = 5  # Maximum retry attempts
    wait_time = 5  # Initial wait time in seconds

    for attempt in range(retries):
        try:
            headers = {"User-Agent": random.choice(USER_AGENTS)}  # Randomize User-Agent
            response = requests.get(url, params=params, headers=headers, timeout=60)

            # Handle 429 (Too Many Requests)
            if response.status_code == 429:
                print(f"Rate limit exceeded. Sleeping for {wait_time} seconds (Attempt {attempt + 1}/{retries})...")
                time.sleep(wait_time + random.uniform(1, 3))  # Add jitter to avoid exact retry intervals
                wait_time *= 2  # Exponential backoff
                continue  # Retry after sleeping

            if response.status_code != 200:
                print(f"Error: {response.status_code} - {response.text}")
                return ""

            data = response.json()
            pages = data.get("query", {}).get("pages", {})

            categories = []
            for key, page_data in pages.items():
                if "categories" in page_data:
                    categories.extend([cat["title"].strip() for cat in page_data["categories"]])

            result = "|".join(categories) if categories else ""
            CATEGORY_CACHE[cache_key] = result  # Cache the result
            return result

        except requests.RequestException as e:
            print(f"Request failed (Attempt {attempt + 1}/{retries}): {e}")
            time.sleep(wait_time + random.uniform(1, 3))  # Add jitter
            wait_time *= 2  # Exponential backoff

    return ""  # Return empty string if all retries fail

# Load the dataset into a Pandas DataFrame
df = pd.read_csv(input_file)

# Ensure we have a unique identifier for merging back later
df["row_id"] = df.index  # Create a unique row ID

# Get language columns dynamically from the user input
language_columns = args.languages.split(",")

# Read from the output file if it exists to avoid re-fetching already fetched categories
if os.path.exists(output_file):
    df_existing = pd.read_csv(output_file)
    # Merge with existing data to retain already fetched categories
    df = df.merge(df_existing, on=df.columns.intersection(df_existing.columns).tolist(), how="left")
    print(f"Loaded existing data from {output_file}, avoiding redundant API requests.")
    
    print(df.columns)

# Function to process each row in parallel
def process_entry(title, lang_col):
    """Wrapper function for multithreading Wikipedia category fetching."""
    return title, get_wikipedia_categories(title, lang_col)

# Process each language in batch mode with multithreading
# Process each language in batch mode with multithreading
# Process each language in batch mode with multithreading
for lang_col in language_columns:
    if lang_col in df.columns:
        print(f"\nProcessing language: {lang_col} ({df[lang_col].notna().sum()} non-null entries)")

        # Extract only non-null and not previously fetched rows
        if f"{lang_col}_categories" in df.columns:
            non_null_entries = df[df[lang_col].notna() & df[f"{lang_col}_categories"].isna()][["row_id", lang_col]].copy()
        else:
            non_null_entries = df[df[lang_col].notna()][["row_id", lang_col]].copy()
            df[f"{lang_col}_categories"] = None  # Ensure the column exists

        if non_null_entries.empty:
            print(f"Skipping {lang_col} as all categories are already fetched.")
            continue

        # non_null_entries = non_null_entries.iloc[:200000]  # Limit batch size

        # Multithreaded Wikipedia category fetching
        results = []
        batch_size = 10000  # Save after every 10k entries
        processed_count = 0

        with ThreadPoolExecutor(max_workers=50) as executor:
            future_to_title = {executor.submit(process_entry, title, lang_col): title for title in non_null_entries[lang_col]}

            for future in tqdm(as_completed(future_to_title), total=len(non_null_entries), desc=f"Fetching {lang_col} categories", unit=" page"):
                try:
                    title, categories = future.result()
                    results.append((title, categories))
                    processed_count += 1

                    # Save every 10,000 processed entries
                    if processed_count % batch_size == 0:
                        results_df = pd.DataFrame(results, columns=[lang_col, f"{lang_col}_categories"])
                        df = df.merge(results_df, on=lang_col, how="left", suffixes=("", "_new"))
                        df[f"{lang_col}_categories"] = df[f"{lang_col}_categories"].fillna(df[f"{lang_col}_categories_new"])
                        df.drop(columns=[f"{lang_col}_categories_new"], inplace=True)
                        df.to_csv(output_file, index=False)
                        print(f"\nSaved intermediate results after {processed_count} entries for {lang_col}.")

                        # Clear the results buffer to free memory
                        results.clear()

                except Exception as e:
                    print(f"Error processing {future_to_title[future]}: {e}")

        # Save remaining results after all entries are processed
        if results:
            results_df = pd.DataFrame(results, columns=[lang_col, f"{lang_col}_categories"])
            df = df.merge(results_df, on=lang_col, how="left", suffixes=("", "_new"))
            df[f"{lang_col}_categories"] = df[f"{lang_col}_categories"].fillna(df[f"{lang_col}_categories_new"])
            df.drop(columns=[f"{lang_col}_categories_new"], inplace=True)
            df.to_csv(output_file, index=False)
            print(f"\nFinal save after processing {lang_col}")

# Drop the helper columns
df.drop(columns=["row_id"], inplace=True)

# Save final results
df.to_csv(output_file, index=False)

print("\nProcessing complete! Final results saved to", output_file)
