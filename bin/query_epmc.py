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
parser.add_argument('--query-batch-size', type=int, default=250, help=argparse.SUPPRESS) # exposed for testing purposes only - do not change
args = parser.parse_args()

# query EuropePMC for publication metadata
max_retries = 5
def query_europepmc(endpoint, request_params, retry_count=0, graceful_exit=False, no_exit=False):
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
            time.sleep(random.randint(1, 30))
            return query_europepmc(endpoint, request_params, retry_count=retry_count+1)
        else:
            sys.stderr.write(f"Error: malformed data returned for {endpoint} / {request_params} after {max_retries} retries\n")
            return {} if no_exit else (sys.exit(0) if graceful_exit else sys.exit(1))

    # Handle empty results
    if data['hitCount'] == 0:
        sys.stderr.write(f"Error: No data found for {request_params}\n")
        return {}

    return data

def query_article_endpoint(id):
    source = 'PMC' if id.startswith('PMC') else 'MED'
    articles_endpoint = f"{epmc_base_url}/article/{source}/{id}"
    article_params = {'format': 'json', 'resultType': 'core'}
    article_data = query_europepmc(articles_endpoint, article_params, no_exit=True)
    return article_data.get('result', {})


# manually create dictionary of indexed accessions, mapped to GBC database resources
accession_types = json.load(open(args.accession_types, 'r'))
epmc_fields = [
    'pmid', 'pmcid', 'title', 'authorList', 'authorString', 'journalInfo', 'grantsList',
    'keywordList', 'meshHeadingList', 'citedByCount', 'hasTMAccessionNumbers'
]
epmc_base_url = "https://www.ebi.ac.uk/europepmc/webservices/rest"

# read input file
input = json.load(open(args.infile, 'r'))
input_ids = list(input.keys())
formatted_data = {}

# make queries in batches of 250 IDs
while input_ids:
    batch = input_ids[:args.query_batch_size]
    input_ids = input_ids[args.query_batch_size:]

    # create query string for batch and ping search endpoint
    query = " OR ".join([f"PMCID:{ext_id}" if ext_id.startswith("PMC") else f"EXT_ID:{ext_id}" for ext_id in batch])
    query = f"({query}) AND (SRC:MED OR SRC:PMC)"
    search_params = {'query': query, 'resultType': 'core', 'format': 'json', 'pageSize': args.query_batch_size}
    epmc_data = query_europepmc(f"{epmc_base_url}/search", search_params)

    # first, process search results
    for result in epmc_data['resultList']['result']:
        ext_id = result['id']
        formatted_data[ext_id] = {}
        formatted_data[ext_id].update({field: result[field] for field in epmc_fields if field in result})
        formatted_data[ext_id]['accessions'] = input.get(ext_id) or input.get(result.get('pmcid'))

        # track which ids were successfully queried
        try:
            if ext_id in batch:
                batch.remove(ext_id)
            elif result.get('pmcid') in batch:
                batch.remove(result.get('pmcid'))
        except ValueError:
            pass

    # if there are any ids left in the batch, query the article endpoint
    # (I think there's a lag in indexing, as there is a mismatch between the search and article endpoints)
    # this is a workaround to get the missing data
    if len(batch) > 0:
        for ext_id in batch[:]:
            article_result = query_article_endpoint(ext_id)
            formatted_data[ext_id] = {}
            formatted_data[ext_id].update({field: article_result[field] for field in epmc_fields if field in article_result})
            formatted_data[ext_id]['accessions'] = input[ext_id]

            # track which ids were successfully queried
            try:
                if ext_id in batch:
                    batch.remove(ext_id)
                elif article_result.get('pmcid') in batch:
                    batch.remove(article_result.get('pmcid'))
            except ValueError:
                pass

with open(args.outfile, 'w') as f:
    json.dump(formatted_data, f, indent=4)