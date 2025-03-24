process QUERY_EUROPEPMC {
    tag "${meta.resource_chunk}"
    label 'process_tiny'
    debug true

    input:
    tuple val(meta), path(acc_json), path(accession_types)

    output:
    tuple(val(meta), path(outfile))

    script:
    outfile = "${acc_json.baseName}.epmc.json"
    """
    query_epmc.py --infile ${acc_json} --outfile ${outfile} \
    --resource ${meta.resource_name} --accession-types ${accession_types}
    """
}