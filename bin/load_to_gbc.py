#!/usr/bin/env python3
import os
import sys
import time
import argparse
import json

import globalbiodata as gbc

from google.cloud.sql.connector import Connector
import pymysql
import sqlalchemy as db


parser = argparse.ArgumentParser(description='Load data to GBC')
parser.add_argument('--json', type=str, help='Path to JSON file with data', required=True)
parser.add_argument('--accession-types', type=str, help='Path to JSON file with accession types', required=True)
parser.add_argument('--resource', type=str, help='Resource name', required=True)
parser.add_argument('--summary', type=str, help='Path for summary file output', required=True)

parser.add_argument('--db', type=str, help='Database to use (format: instance_name/db_name)', required=True)
parser.add_argument('--dbcreds', type=str, help='Path to JSON file with SQL credentials')
parser.add_argument('--sqluser', type=str, help='SQL user', default=os.environ.get("CLOUD_SQL_USER"))
parser.add_argument('--sqlpass', type=str, help='SQL password', default=os.environ.get("CLOUD_SQL_PASSWORD"))

parser.add_argument('--debug', action='store_true', help='Debug mode')
# parser.add_argument('--dry-run', action='store_true', help='Dry run mode')

args = parser.parse_args()

# setup SQL connection
sqluser, sqlpass = None, None
if args.dbcreds:
    creds = json.load(open(args.dbcreds, 'r'))
    sqluser, sqlpass = creds.get('user'), creds.get('pass')
elif args.sqluser and args.sqlpass:
    sqluser, sqlpass = args.sqluser, args.sqlpass
else:
    sys.exit("Error: No SQL credentials provided")

gcp_connector = Connector()
instance, db_name = args.db.split('/')
def getcloudconn() -> pymysql.connections.Connection:
    conn: pymysql.connections.Connection = gcp_connector.connect(
        instance, "pymysql",
        user=sqluser,
        password=sqlpass,
        db=db_name
    )
    return conn

cloud_engine = db.create_engine("mysql+pymysql://", creator=getcloudconn, pool_recycle=60 * 5, pool_pre_ping=True)

# lazy loading for GBC resources
def get_gbc_resource(dbname):
    name_mapping = {
        'Electron Microscopy Data Bank': 'emdb',
        'IGSR/1000 Genomes': 'igsr', 'Complex Portal': 'complexportal',
        'European Genome-Phenome Archive': 'ega',
        'ClinicalTrials.gov': 'nct', 'EU Clinical Trials Register': 'eudract',
        'MGnify': 'metagenomics',
    }
    mapped_dbname = dbname.lower() if accession_types.get(dbname.lower()) else name_mapping.get(dbname)

    if isinstance(accession_types.get(mapped_dbname), int):
        gbc_resource = gbc.fetch_resource({'id': accession_types.get(mapped_dbname)}, expanded=False, engine=cloud_engine)
        accession_types[mapped_dbname] = gbc_resource
        if not gbc_resource:
            raise ValueError(f"Error: No GBC resource found for {dbname} (-> {mapped_dbname})")
        return gbc_resource
    else:
        try:
            return accession_types[mapped_dbname]
        except KeyError:
            raise KeyError(f"Error: No GBC resource found for {dbname} (-> {mapped_dbname})")


# import dictionary of indexed accessions, mapped to GBC database resources
accession_types = json.load(open(args.accession_types, 'r'))

prediction = gbc.Prediction({
    'name': 'EuropePMC text-mined accession loading',
    'date': '2025-03-24',
    'user': 'carlac'
})
prediction.write(engine=cloud_engine, debug=args.debug)

gbc_db = get_gbc_resource(args.resource)

publications = json.load(open(args.json, 'r'))
summary_out = open(args.summary, 'w')

max_time, min_time = 0, 0
for pub in publications:
    summary_out.write("---------------------------------------------------------------\n")
    summary_out.write(f"📖 {pub.get('title')} (PMID: {pub.get('pmid', 'NA')})\n")


    t0 = time.time()
    if pub.get('pmid'):
        gbc_pub = gbc.fetch_publication({'pubmed_id': pub.get('pmid')}, engine=cloud_engine, debug=args.debug, expanded=False)
    else:
        gbc_pub = gbc.fetch_publication({'pmc_id': pub.get('pmcid')}, engine=cloud_engine, debug=args.debug, expanded=False)
    t1 = time.time()

    if not gbc_pub:
        t0 = time.time()
        gbc_pub = gbc.new_publication_from_EuropePMC_result(pub, google_maps_api_key=os.environ.get('GOOGLE_MAPS_API_KEY'))
        t1 = time.time()
        gbc_pub.write(engine=cloud_engine, debug=args.debug)
        summary_out.write("    ✅ New publication written to database\n")
    else:
        summary_out.write("    🔍 Publication already exists in database\n")

    t2 = time.time()

    summary_out.write(f"1. Creation of gbc.Publication object: {round(t1-t0, 3)}s\n")
    summary_out.write(f"2. Writing publication to database: {round(t2-t1, 3)}s\n")

    for acc in pub.get('accessions'):
        gbc_acc = gbc.Accession({
            'accession': acc, 'resource': gbc_db,
            'prediction': prediction, 'publications': [gbc_pub]
        })
        gbc_acc.write(engine=cloud_engine, debug=args.debug)

    summary_out.write(f"🔗 New {args.resource} data links:{len(pub.get('accessions'))}\n")

    t4 = time.time()
    this_time = t4-t0
    max_time = max(max_time or this_time, this_time)
    min_time = min(min_time or this_time, this_time)

summary_out.write("\n")
summary_out.write("📊 Summary of data loading:\n")
summary_out.write(f"📈 Total number of publications loaded: {len(publications)}\n")
summary_out.write(f"🕓 Maximum time taken for a publication: {round(max_time, 3)}s\n")
summary_out.write(f"🕓 Minimum time taken for a publication: {round(min_time, 3)}s\n")
summary_out.close()