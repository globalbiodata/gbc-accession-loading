process DOWNLOAD_TM_FILES {
    label 'process_tiny'
    debug true

    container 'europe-west2-docker.pkg.dev/gbc-publication-analysis/gbc-docker/bash-wget:latest'

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
    for et in ${excluded_types}; do
        rm -f ${outdir}/\$et.csv
    done
    """
}