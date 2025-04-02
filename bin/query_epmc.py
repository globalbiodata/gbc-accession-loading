#!/usr/bin/env python3
import sys
import json

import requests
import time
import random

import argparse
parser = argparse.ArgumentParser(description='Query EuropePMC for publication metadata.')
parser.add_argument('--infile', type=str, help='JSON file of text mined accessions', required=True)
parser.add_argument('--resource', type=str, help='Resource name', required=True)
parser.add_argument('--accession-types', type=str, help='Path to JSON file with accession types', required=True)
parser.add_argument('--outfile', type=str, help='Output directory for results', required=True)
parser.add_argument('--query-batch-size', type=int, default=250, help=argparse.SUPPRESS)
args = parser.parse_args()

# query EuropePMC for publication metadata
max_retries = 5
def query_europepmc(endpoint, request_params, retry_count=0, graceful_exit=False):
    response = requests.get(endpoint, params=request_params)
    if response.status_code == 200:
        data = response.json()
    else:
        sys.stderr.write(f"Error: {response.status_code} - {response.text}\n")
        sys.exit(1)

    # Handle malformed/incomplete results - retry up to max_retries times
    if not data.get('hitCount'):
        sys.stderr.write(f"Error: No data found for {endpoint} / {request_params}. Retrying...\n")
        if retry_count < max_retries:
            time.sleep(random.randint(1, 15))
            return query_europepmc(endpoint, request_params, retry_count=retry_count+1)
        else:
            sys.stderr.write(f"Error: No data found for {endpoint} / {request_params} after {max_retries} retries\n")
            sys.exit(0) if graceful_exit else sys.exit(1)

    # Handle empty results
    if data['hitCount'] == 0:
        sys.stderr.write(f"Error: No data found for {request_params}\n")
        return {}

    return data

# manually create dictionary of indexed accessions, mapped to GBC database resources
accession_types = json.load(open(args.accession_types, 'r'))
epmc_fields = [
    'pmid', 'pmcid', 'title', 'authorList', 'authorString', 'journalInfo', 'grantsList',
    'keywordList', 'meshHeadingList', 'citedByCount', 'hasTMAccessionNumbers'
]
epmc_base_url = "https://www.ebi.ac.uk/europepmc/webservices/rest"

# read input file
input = json.load(open(args.infile, 'r'))

# make queries in batches of 250 IDs
input_ids = list(input.keys())
formatted_data = {}

while input_ids:
    batch = input_ids[:args.query_batch_size]
    input_ids = input_ids[args.query_batch_size:]

    # create query string for batch and search
    query = " OR ".join([f"EXT_ID:{ext_id}" for ext_id in batch])
    search_params = {'query': query, 'resultType': 'core', 'format': 'json'}
    epmc_data = query_europepmc(f"{epmc_base_url}/search", search_params)

    for result in epmc_data['resultList']['result']:
        ext_id = result['id']
        formatted_data[ext_id] = {}
        for field in epmc_fields:
            if result.get(field):
                formatted_data[ext_id][field] = result.get(field)
        formatted_data[ext_id]['accessions'] = input[ext_id]

with open(args.outfile, 'w') as f:
    json.dump(formatted_data, f, indent=4)