import re
import csv
import json
import sqlite3
from sys import exit
from time import sleep
from pathlib import Path
from urllib import request
from urllib.request import urlretrieve
from Bio import Phylo

OTT_RELEASE = "15.1" # See https://files.opentreeoflife.org/synthesis/
OTT_API_URL = "https://api.opentreeoflife.org/v3"
OTT_TREE_URL = f'https://files.opentreeoflife.org/synthesis/opentree{OTT_RELEASE}/output/labelled_supertree/labelled_supertree.tre'
OTT_TREE_FILE = Path(f"tree-{OTT_RELEASE}.tre")
DB_FILE = Path(f"taxa-{OTT_RELEASE}.db")
SCHEMA = ["id", "parent", "name", "extant", "other_names", "description"]

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
	# Flatten the phylogenetic tree of life into a list, one item per taxon.
	# Data from opentreeoflife.org is formatted as a Newick tree string.
	print("Processing tree")
	taxa = []
	tree = Phylo.read(OTT_TREE_FILE, "newick")
	stack = [(None, tree.root)]
	while stack:
		parent, clade = stack.pop()
		if clade.name:
			taxa.append((clade.name, parent))
		for child in clade.clades:
			stack.append((clade.name, child))
	return taxa

def db_exists(conn):
	return bool(conn.execute("PRAGMA table_info(taxa);").fetchone())

def db_create(conn, taxa):
	# Taxa list is saved to a SQLite3 database at DB_FILE e.g. taxa-15.1.db
	# At this point we only have relationships without any details.
	# Table creation will rollback if an exception is raised inside the "with" block.
	print("Creating database")
	with conn:
		conn.execute("""
			CREATE TABLE IF NOT EXISTS taxa (
				id     TEXT PRIMARY KEY,
				parent TEXT,
				info   TEXT DEFAULT NULL
			)
		""")
		conn.executemany("INSERT INTO taxa (id, parent) VALUES (?, ?)", taxa)

def lookup_taxa(conn):
	# Use the opentreeoflife.org API to get lookup each taxon.
	# See https://github.com/OpenTreeOfLife/germinator/wiki/Open-Tree-of-Life-Web-APIs
	print("Looking up taxa...")
	url = f"{OTT_API_URL}/taxonomy/taxon_info"
	opener = request.build_opener(request.HTTPSHandler())
	headers = {"Content-Type": "application/json"}
	pattern = re.compile(r"^ott\d+$")
	total = conn.execute("SELECT COUNT(*) FROM taxa WHERE info IS NULL").fetchone()[0]
	new_taxa = conn.execute("SELECT id FROM taxa WHERE info IS NULL")
	for index, (taxon_id,) in enumerate(new_taxa, start=1):

		# Show progress in the console.
		percent = (index / total) * 100
		print(f"   Processing taxon {index} of {total} ({percent:.2f}%)", end="\r", flush=True)

		# Avoid entries with a composite label i.e. it's not in the OTT dataset.
		if not pattern.match(taxon_id):
			conn.execute(
				"UPDATE taxa SET info=? WHERE id=?",
				(json.dumps({"name": "Unknown Taxon"}), taxon_id)
			)
			continue

		# Look up data using the API and save the results to our database.
		payload = json.dumps({"ott_id": int(taxon_id[3:])}).encode("utf-8")
		req = request.Request(url, data=payload, headers=headers)
		with opener.open(req) as response:
			if response.status == 200:
				taxon_info = json.loads(response.read().decode("utf-8"))
				conn.execute(
					"UPDATE taxa SET info=? WHERE id=?",
					(json.dumps(taxon_info), taxon_id)
				)

		# Commit in batches of 100 i.e. about every minute.
		if index % 100 == 0:
			conn.commit()

		sleep(0.5) # Respectful API call velocity.

def main():
	conn = sqlite3.connect(DB_FILE)
	conn.execute("PRAGMA journal_mode=WAL;")
	if not db_exists(conn):
		download_tree()
		taxa = process_tree()
		db_create(conn, taxa)
	try:
		lookup_taxa(conn)
	except KeyboardInterrupt:
		print('\nAborted!')
		exit(130) # Our job is incomplete, but the script can be safely run again to resume.
	conn.close()
	print("Done!")

if __name__ == "__main__":
	main()
