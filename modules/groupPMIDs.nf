process GROUP_PMIDS {
    tag "${meta.resource_name}"
    label 'process_tiny'
    debug true

    input:
    tuple val(meta), path(csv_file), val(batch_size)

    output:
    tuple(val(meta), path("${outdir}/**.json"))

    script:
    outdir = "${meta.resource_name}_grouped"
    """
    group_pmids.py --csv-file ${csv_file} --outdir ${outdir} \
    --prefix ${meta.resource_name} --batch-size ${batch_size}
    """
}