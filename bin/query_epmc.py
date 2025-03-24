#!/usr/bin/env python3
import sys
import os

import requests
import json
import time
import random
import argparse

from google.cloud.sql.connector import Connector
import pymysql
import sqlalchemy as db

parser = argparse.ArgumentParser(description='Query EuropePMC for accession data.')
parser.add_argument('--csv-file', type=str, help='CSV file of text mined accessions', required=True)
parser.add_argument('--resource', type=str, help='Resource name', required=True)
parser.add_argument('--accession-types', type=str, help='Path to JSON file with accession types', required=True)
parser.add_argument('--outdir', type=str, help='Output directory for results', required=True)
parser.add_argument('--batch-size', type=int, default=1000, help='Number of results per page')

args = parser.parse_args()
limit = args.limit if args.limit > 0 else None

if not os.path.exists(args.outdir):
    os.makedirs(args.outdir)

max_retries = 10
def query_europepmc(endpoint, request_params, retry_count=0, graceful_exit=False):
    response = requests.get(endpoint, params=request_params)
    if response.status_code == 200:
        data = response.json()

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

# hash directory for results files (to avoid overpopulating a single directory)
def hashed_json_filename(c):
    cstr = str(c).zfill(7)
    hashdir = '/'.join(list(cstr)[::-1][:4])
    outdir = f"{args.outdir}/{hashdir}"
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    return f"{outdir}/results.{cstr}.json"

# manually create dictionary of indexed accessions, mapped to GBC database resources
accession_types = json.load(open(args.accession_types, 'r'))
acc_query = "(%s)" % ' OR '.join([f"ACCESSION_TYPE:{at}" for at in accession_types])
epmc_fields = [
    'pmid', 'pmcid', 'title', 'authorList', 'authorString', 'journalInfo', 'grantsList',
    'keywordList', 'meshHeadingList', 'citedByCount', 'hasTMAccessionNumbers'
]

epmc_base_url = "https://www.ebi.ac.uk/europepmc/webservices/rest"
more_data = True
while more_data:
    search_params = {
        'query': acc_query, 'resultType': 'core',
        'format': 'json', 'pageSize': args.page_size
    }

    # if error, exit gracefully : this allows us to resume from where we left off in the
    # event of an error (using the last cursor mark in the database)
    data = query_europepmc(f"{epmc_base_url}/search", search_params, graceful_exit=True)

    limit = limit or data.get('hitCount')
    if cursor is None:
        print(f"--- Expecting {limit} of {data.get('hitCount')} results!")

    formatted_results = {'cursor': cursor, 'results': []}
    for result in data['resultList']['result']:
        formatted_results['results'].append({k: result[k] for k in epmc_fields if k in result})

    with(open(generate_json_file(c), 'w')) as f:
        json.dump(formatted_results, f, indent=4)
        print(f"---- Wrote {args.page_size} results to file {f.name} (cursor: {cursor})")

    c += 1
    limit -= args.page_size
    print(f"----- Remaining results: {limit}")

    cursor = data.get('nextCursorMark')
    if not cursor or limit <= 0:
        more_data = False