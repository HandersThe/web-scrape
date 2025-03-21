import pandas as pd
import json
import os
import time
from serpapi import GoogleSearch

# Function to extract first address, phone, and website
def extract_info(search_params):
    try:
        search = GoogleSearch(search_params)
        results = search.get_dict()
        
        # Handle both 'place_results' and 'local_results' structures
        if "place_results" in results:
            data = results["place_results"]
            address = data.get("address", "Not available")
            phone = data.get("phone", "Not available")
            website = data.get("website", "Not available")
        elif "local_results" in results and results["local_results"]:
            data = results["local_results"][0]  # First result
            address = data.get("address", "Not available")
            phone = data.get("phone", "Not available")
            website = data.get("website", "Not available")
        else:
            return {"address": "Not found", "phone": "Not found", "website": "Not found"}
        
        return {"address": address, "phone": phone, "website": website}
    except Exception as e:
        print(f"Error processing {search_params['q']}: {e}")
        return {"address": "Error", "phone": "Error", "website": "Error"}

# Load API key from file
api_key_file_path = "api_key.txt"  # Replace with your API key file path
try:
    with open(api_key_file_path, "r", encoding="utf-8") as api_file:
        api_key = api_file.read().strip()  # Read and remove any whitespace
    print(f"Loaded API key from {api_key_file_path}")
except FileNotFoundError:
    raise FileNotFoundError(f"API key file not found at {api_key_file_path}")

# Base parameters for SerpApi
base_params = {
    "engine": "google_maps",
    "type": "search",
    "ll": "@-6.233671004133035,106.63697905716464,14z",  # Modify if needed
    "google_domain": "google.com",
    "hl": "en",
    "api_key": api_key  # Use loaded API key
}

# Path for cache file
cache_file_path = "cache.json"

# Load existing cache if it exists
cache = {}
if os.path.exists(cache_file_path):
    with open(cache_file_path, "r", encoding="utf-8") as cache_file:
        cache = json.load(cache_file)
    print(f"Loaded cache from {cache_file_path} with {len(cache)} entries")
else:
    print("No existing cache found, starting fresh")

# Read the CSV file with pandas
csv_file_path = "company_list.csv"  # Replace with your CSV file path
df = pd.read_csv(csv_file_path)
if "Company Name" not in df.columns:
    raise ValueError("CSV must have a 'Company Name' column")

# List to store results
results_list = []

# Process each name
for name in df["Company Name"]:
    # Check if name is already cached
    if name in cache:
        print(f"Using cached result for: {name}")
        info = cache[name]
    else:
        print(f"Processing: {name}")
        # Update query in parameters
        params = base_params.copy()
        params["q"] = name
        
        # Get info, cache it, and add delay
        info = extract_info(params)
        cache[name] = info
        time.sleep(1)  # 1-second delay for new API calls
    
    # Add name and extracted info to results
    results_list.append({
        "name": name,
        "address": info["address"],
        "phone": info["phone"],
        "website": info["website"]
    })

# Save updated cache to file
with open(cache_file_path, "w", encoding="utf-8") as cache_file:
    json.dump(cache, cache_file, ensure_ascii=False, indent=2)
print(f"Cache saved to {cache_file_path}")

# Convert results to DataFrame and save to CSV
output_df = pd.DataFrame(results_list)
output_file_path = "company_information_enriched.csv"
output_df.to_csv(output_file_path, index=False, encoding="utf-8")
print(f"Results saved to {output_file_path}")