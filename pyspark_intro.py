from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import requests
import pandas as pd

# Create a SparkSession
spark = SparkSession.builder \
    .appName("WikiIntroExtractor") \
    .getOrCreate()

# Define the User-Agent
USER_AGENT = "WikiIntroExtractor/1.0 (Sourabarata Mukherjee; soura1990@gmail.com)"

# Function to fetch introductory text for a Wikipedia URL
def get_introductory_text(wiki_url):
    if not wiki_url or wiki_url is None:
        return None
        # Extract the title from the URL
    encoded_title = wiki_url.split("/")[-1]
    title = requests.utils.unquote(encoded_title)
    
    # API endpoint and parameters
    api_url = f"https://{wiki_url.split('/')[2]}/w/api.php"
    params = {
            "action": "query",
            "prop": "extracts",
            "exintro": True,
            "explaintext": True,
            "titles": title,
            "format": "json",
        }
    headers = {"User-Agent": USER_AGENT}
    
    # Make the API request
    response = requests.get(api_url, headers=headers, params=params, timeout=60)
    
    if response.status_code != 200:
        print(response)
        return None
    data = response.json()
    pages = data.get("query", {}).get("pages", {})
    page = next(iter(pages.values()))
    intro = page.get("extract", None)
    
    if intro == "":
        print("Empty intro")
        return None
    
    return intro


# Register UDF for PySpark
get_introductory_text_udf = udf(get_introductory_text, StringType())

# Load the CSV into a Spark DataFrame
global_df = spark.read.csv("wikidata_results_fix.csv", header=True)
global_df  = global_df.limit(100)

# For each language, fetch introductory text
for lang in ['en', 'hi', 'bn', 'ja', 'ko', 'mr', 'ta', 'jv', 'ne', 'ur', 'th', 'bpy']:
    global_df = global_df.withColumn(f"{lang}_intro", get_introductory_text_udf(col(lang)))

# Save the final DataFrame to CSV
global_df.write.csv("final_pages_spark_fix", header=True, mode="overwrite")
