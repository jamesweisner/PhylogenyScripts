import re
import csv
import json
from sys import exit
from time import sleep, time
from pathlib import Path
from urllib import request
from urllib.request import urlretrieve
from Bio import Phylo

OTT_RELEASE = "15.1" # See https://files.opentreeoflife.org/synthesis/
OTT_API_URL = "https://api.opentreeoflife.org/v3"
OTT_TREE_URL = f'https://files.opentreeoflife.org/synthesis/opentree{OTT_RELEASE}/output/labelled_supertree/labelled_supertree.tre'
OTT_TREE_FILE = Path(f"tree-{OTT_RELEASE}.tre")
CLADES_FILE = Path(f"clades-{OTT_RELEASE}.csv")
SCHEMA = ["id", "parent", "name", "extant", "other_names", "description"]

clades = []

def download_tree():
	# Download the opentreeoflife.org phylogenetic tree of life.
	# Their "synthetic release" combines several sources into a single tree.
	# They make periodic releases, so check that OTT_RELEASE is up to date
	# Save the file locally to OTT_TREE_FILE e.g. tree-15.1.tre
	# If that file already exists, the download step is skipped.
	# If that file is bad, you can safely delete it and it will be re-downloaded.
	if OTT_TREE_FILE.exists():
		print(f"Found {OTT_TREE_FILE}")
		return
	print(f"Downloading {OTT_TREE_FILE}")
	try:
		urlretrieve(OTT_TREE_URL, OTT_TREE_FILE)
	except Exception as e:
		print("Download failed:", e)
		exit(1) # This is a fatal error.

def process_tree():
	# Flatten the phylogenetic tree of life into clades.
	# Data from opentreeoflife.org is formatted as a Newick tree string.
	# The result is saved to CLADES_FILE in CSV format e.g. clades-15.1.csv
	# At this point we only have relationships without any details.
	print("Processing tree")
	tree = Phylo.read(OTT_TREE_FILE, "newick")
	stack = [(None, tree.root)]
	while stack:
		parent, clade = stack.pop()
		if clade.name:
			clades.append({"id": clade.name, "parent": parent})
		for child in clade.clades:
			stack.append((clade.name, child))

def load_clades():
	if not CLADES_FILE.exists():
		return False
	print(f"Loading {CLADES_FILE}")
	with open(CLADES_FILE, newline="") as f:
		for row in csv.DictReader(f):
			clades.append(row)
	return True

def save_clades():
	with open(CLADES_FILE, "w", newline="") as file:
		writer = csv.writer(file)
		writer.writerow(SCHEMA)
		writer.writerows([clade.get(k) for k in SCHEMA] for clade in clades)

def lookup_clades():
	# Use the opentreeoflife.org API to get lookup each clade.
	# See https://github.com/OpenTreeOfLife/germinator/wiki/Open-Tree-of-Life-Web-APIs
	print("Looking up clades...")
	url = f"{OTT_API_URL}/taxonomy/taxon_info"
	opener = request.build_opener(request.HTTPSHandler())
	headers = {"Content-Type": "application/json"}
	pattern = re.compile(r"^ott\d+$")
	total = len(clades)
	count = 0
	seconds = 0
	for index, clade in enumerate(clades):

		# Show progress every second.
		if seconds != int(time()):
			percent = (index / len(clades)) * 100
			print(f"   Processing clade {index} of {len(clades)} ({percent:.1f}%)", end="\r", flush=True)
			seconds = int(time())

		# Save periodically just in case.
		if count > 99:
			save_clades()
			count = 0

		if clade.get("name"):
			continue # Already have info on this clade.
		if not pattern.match(clade["id"]):
			clade["name"] = "Unknown Taxon"
			continue # This clade has a composite label i.e. not in OTT dataset.
		payload = json.dumps({"ott_id": int(clade["id"][3:])}).encode("utf-8")
		req = request.Request(url, data=payload, headers=headers)
		with opener.open(req) as response:
			if response.status == 200:
				taxon = json.load(response)
				clade["name"] = taxon.get("name", "Unnamed Clade")
				clade["extant"] = not any(flag in taxon.get("flags", []) for flag in ["extinct", "extinct_inherited"])
				clade["other_names"] = json.dumps(taxon.get("synonyms"))
				clade["description"] = "\n".join([
					"Rank: " + taxon.get("rank"),
					"Sources: " + ", ".join(taxon.get("tax_sources")),
				])
		count += 1
		sleep(0.5) # Respectful API call velocity.

def main():
	if not load_clades():
		download_tree()
		process_tree()
		save_clades()
	try:
		lookup_clades()
	except KeyboardInterrupt:
		print('\nAborted!')
		exit(130) # Our job is incomplete, but the script can be safely run again to resume.
	save_clades()
	print("Done!")

if __name__ == "__main__":
	main()
