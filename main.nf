nextflow.enable.dsl=2

// Run the workflow
include { DOWNLOAD_TM_FILES } from './modules/downloadTMFiles'
include { GROUP_PMIDS } from './modules/groupPMIDs'
include { QUERY_EUROPEPMC } from './modules/queryEuropePMC'
include { WRITE_TO_GBC } from './modules/writeToGBC'


workflow {
    main:

        download = DOWNLOAD_TM_FILES(params.download_url, params.excluded_types, params.wget_options)
        download.accession_csvs | flatten
        | view

        download.accession_csvs
        | flatten
        | map { csv ->
            def meta = ['resource_name': csv.baseName, 'accession_count': csv.countLines()-1]
            [meta, csv, params.batch_size]
        }
        | GROUP_PMIDS
        | set { grouped_accessions }


        // take tuple of meta and jsons list, and convert to list of tuples of meta, json, accession_types
        // grouped_accessions
        // | flatMap { meta, jsons ->
        //     def json_list = jsons instanceof List ? jsons : [jsons]
        //     json_list.collect { json ->
        //         [meta + ['resource_chunk':json.baseName], json, file(params.accession_types)]
        //     }
        // }
        // | view

        grouped_accessions
        | flatMap { meta, jsons ->
            def json_list = jsons instanceof List ? jsons : [jsons]
            json_list.collect { json ->
                [meta + ['resource_chunk':json.baseName], json, file(params.accession_types)]
            }
        }
        | QUERY_EUROPEPMC
        | set { epmc_jsons }

        // epmc_jsons
        // | view

        epmc_jsons
        | map { meta, json ->
            [meta, json, file(params.accession_types), params.db, file(params.db_creds), file(params.version_json)]
        }
        | WRITE_TO_GBC
}
