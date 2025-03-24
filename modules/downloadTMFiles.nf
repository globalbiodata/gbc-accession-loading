process DOWNLOAD_TM_FILES {
    label 'process_tiny'
    debug true

    input:
    val(download_url)
    val(excluded_types)
    val(wget_options)

    output:
    path("text_mined_csvs/*.csv"), emit: accession_csvs

    script:
    outdir = "text_mined_csvs"
    """
    wget -r -l 1 ${wget_options} -nd -e robots=off ${download_url} -P ${outdir}
    rm ${excluded_types}
    """
}