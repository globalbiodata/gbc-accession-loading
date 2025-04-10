#!/usr/bin/env python3
import argparse
import json

import pandas as pd
import os

parser = argparse.ArgumentParser(description='Query EuropePMC for accession data.')
parser.add_argument('--csv-file', type=str, help='CSV file of text mined accessions', required=True)
parser.add_argument('--outdir', type=str, help='Output directory for results', required=True)
parser.add_argument('--prefix', type=str, default='split', help='Prefix for output files (default: split)')
parser.add_argument('--batch-size', type=int, default=250, help='Number IDs per file')

args = parser.parse_args()

if not os.path.exists(args.outdir):
    os.makedirs(args.outdir)

# hash directory for results files (to avoid overpopulating a single directory)
def hashed_json_filename(c):
    cstr = str(c).zfill(6)
    hashdir = '/'.join(list(cstr)[::-1][:3])
    outdir = f"{args.outdir}/{hashdir}"
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    return f"{outdir}/{args.prefix}_{cstr}.accessions.json"


df = pd.read_csv(args.csv_file, header=0, names=['accession','pmc_id','ext_id', 'source'])
df = df[df['source'].isin(['MED', 'PMC'])]
grouped = df.groupby('ext_id')['accession'].apply(list).reset_index()
result = grouped.set_index('ext_id').to_dict()['accession']

summary_out = open(f"{args.outdir}/{args.prefix}.summary.tsv", 'w')
summary_out.write("batch\tuniq_pub_count\tuniq_acc_count\tuniq_pub_acc_combo\n")
for i in range(0, len(result), args.batch_size):
    batch = dict(list(result.items())[i:i + args.batch_size])
    batch_file = hashed_json_filename((i // args.batch_size) + 1)
    with open(batch_file, 'w') as f:
        json.dump(batch, f, indent=4)

    summary_out.write("\t".join(
        f"{os.path.basename(batch_file)}",
        len(batch),
        len(set(acc for acc_list in batch.values() for acc in acc_list)),
        sum(len(v) for v in batch.values()))
    + "\n")
summary_out.close()