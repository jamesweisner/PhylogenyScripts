from csv import DictReader, DictWriter
from requests import post, RequestException
from time import sleep

"""
	SELECT DISTINCT ON (TRIM(name))
		id,
		TRIM(name) AS name,
		COALESCE(other_names,  '') AS other_names,
		COALESCE(extant::text, '') AS extant,
		COALESCE(description,  '') AS description
	FROM clades
	ORDER BY TRIM(name), created_at DESC
"""

INPUT_FILE  = "clades.csv"
OUTPUT_FILE = "clades-with-ott-id.csv"
API_URL = "https://api.opentreeoflife.org/v3/tnrs/match_names"
BATCH_SIZE = 100

with open(INPUT_FILE, newline="", encoding="utf-8") as file:
	print(f"Loading {INPUT_FILE}")
	clades = list(DictReader(file))
	print(f"Loaded {len(clades)} clades")

row_lookup = {c["name"]: i for i, c in enumerate(clades) if not c.get("ott_id")}
total = len(row_lookup.keys())
print(f"Looking up ott_id for {total} clades")

# Look up clades in batches using the Open Tree of Life API.
# See https://github.com/OpenTreeOfLife/germinator/wiki/TNRS-API-v3#match_names
stats = [0, 0, 0]
names = list(row_lookup.keys())
for i in range(0, total, BATCH_SIZE):
	percent = (i / total) * 100
	print(f"   Processed {i} of {total} ({percent:.2f}%)", end="\r", flush=True)
	try:
		response = post(API_URL, json={"names": names[i:i + BATCH_SIZE]}, timeout=10)
		response.raise_for_status()
		result = response.json()
	except RequestException as e:
		print(f"\nAPI call failed: {e}")
		exit(1)
	for clade in result["results"]:
		row = clades[row_lookup[clade["name"]]]
		if len(clade["matches"]) == 0:
			row["ott_id"] = -1
			stats[1] += 1 # No matches
		elif len(clade["matches"]) > 1:
			row["ott_id"] = -2
			stats[2] += 1 # Multiple matches
		else:
			row["ott_id"] = clade["matches"][0]["taxon"]["ott_id"]
			stats[0] += 1 # Scucessful lookup
	sleep(0.5) # Respectful API call velocity.

print(f"Found {stats[0]}, missing {stats[1]}, ambiguous {stats[2]}")

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as file:
	writer = DictWriter(file, fieldnames=clades[0].keys())
	writer.writeheader()
	writer.writerows(clades)
