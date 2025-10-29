import requests
import pandas as pd
import os


API_KEY = "527bee2ef95f18d0fb266fc716130f05db1e5c7a"

# Folder for saving CSVs
output_folder = "census_data"
os.makedirs(output_folder, exist_ok=True)

# Census year endpoints and variable names
census_info = {
    2000: {"url": "https://api.census.gov/data/2000/dec/sf1", "var": "P001001"},
    2010: {"url": "https://api.census.gov/data/2010/dec/sf1", "var": "P001001"},
    2020: {"url": "https://api.census.gov/data/2020/dec/pl", "var": "P1_001N"},
}

# Keep track of all DataFrames for merging
all_dfs = []

# Loop through each decade
for year, info in census_info.items():
    print(f"\n📡 Fetching Census data for {year}...")
    params = {
        "get": f"NAME,{info['var']}",
        "for": "state:*",
        "key": API_KEY
    }

    response = requests.get(info["url"], params=params)

    # Handle failed request
    if response.status_code != 200:
        print(f" Request failed ({response.status_code}): {response.text}")
        continue

    try:
        data = response.json()
    except Exception as e:
        print(f" JSON Decode Error for {year}: {e}")
        print("Raw response:\n")
        print(response.text[:500])
        continue

    # Convert to DataFrame
    df = pd.DataFrame(data[1:], columns=data[0])
    df.rename(columns={info["var"]: "Total_Population"}, inplace=True)
    df["Year"] = year  # Add year column
    df["Total_Population"] = df["Total_Population"].astype(int)

    # Save each decade's CSV
    file_path = os.path.join(output_folder, f"census_{year}.csv")
    df.to_csv(file_path, index=False)
    print(f" Saved {year} data to {file_path}")

    all_dfs.append(df)

#  Merge all decades into one DataFrame
if all_dfs:
    merged_df = pd.concat(all_dfs, ignore_index=True)
    merged_df = merged_df[["Year", "NAME", "state", "Total_Population"]]
    merged_path = os.path.join(output_folder, "merged_census_data.csv")
    merged_df.to_csv(merged_path, index=False)
    print(f"\n Merged dataset saved as {merged_path}")
else:
    print("\n No data available to merge.")

print("\n All available Census data downloaded and merged successfully!")


# --------  add Region column using a non-repeating mapping --------
# Load the merged file produced above
merged_input = os.path.join(output_folder, "merged_census_data.csv")
df_regions = pd.read_csv(merged_input)

# Define states once per region
regions = {
    "Northeast": [
        # New England
        "Connecticut", "Maine", "Massachusetts", "New Hampshire", "Rhode Island", "Vermont",
        # Mid-Atlantic
        "New Jersey", "New York", "Pennsylvania",
    ],
    "Midwest": [
        # East North Central
        "Illinois", "Indiana", "Michigan", "Ohio", "Wisconsin",
        # West North Central
        "Iowa", "Kansas", "Minnesota", "Missouri", "Nebraska", "North Dakota", "South Dakota",
    ],
    "South": [
        # South Atlantic
        "Delaware", "District of Columbia", "Florida", "Georgia", "Maryland",
        "North Carolina", "South Carolina", "Virginia", "West Virginia",
        # East South Central
        "Alabama", "Kentucky", "Mississippi", "Tennessee",
        # West South Central
        "Arkansas", "Louisiana", "Oklahoma", "Texas",
    ],
    "West": [
        # Mountain
        "Arizona", "Colorado", "Idaho", "Montana", "Nevada", "New Mexico", "Utah", "Wyoming",
        # Pacific
        "Alaska", "California", "Hawaii", "Oregon", "Washington",
    ],
}

# Flatten: state -> region mapping
region_map = {state: region for region, states in regions.items() for state in states}

# Add Region; anything not listed becomes "undefined" (e.g., Puerto Rico)
df_regions["Region"] = df_regions["NAME"].map(region_map).fillna("undefined")

# Save as a new file (keep original merged CSV intact)
output_with_region = os.path.join(output_folder, "merged_census_data_with_region.csv")
df_regions.to_csv(output_with_region, index=False)

print(f"✅ Added 'Region' column and saved to: {output_with_region}")
# ------------------------------------------------------------------------
