process WRITE_TO_GBC {
    tag "${meta.resource_chunk}"
    label 'process_tiny'
    debug true

    input:
    tuple val(meta), path(json_file), path(accession_types), val(db), path(db_creds)

    output:
    path(summary_file)

    script:
    summary_file = "${json_file.baseName}.summary.txt"
    """
    load_to_gbc.py --json ${json_file} --accession-types ${accession_types} --resource ${meta.resource_name} \
    --summary ${summary_file} --db ${db} --dbcreds ${db_creds}
    """
}