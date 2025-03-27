from functools import cache


def table_map():
    """
    Built using the script `scripts/make_table_map.py`
    """
    return {
        "data": {
            "data_id": "data.id",
            "study_id": "study_id",
            "lims_qc": "lims_qc",
            "qc_date": "date",
            "tag1_id": "tag1_id",
            "tag2_id": "tag2_id",
        },
        "file": {
            "data_id": "data.id",
            "remote_path": "remote_path",
        },
        "library": {
            "pipeline_id_lims": "library_type_id",
            "library_id": "library.id",
        },
        "pacbio_run_metrics": {
            "run_id": "pacbio_run_metrics.id",
            "movie_minutes": "movie_minutes",
            "binding_kit": "binding_kit",
            "sequencing_kit": "sequencing_kit",
            "sequencing_kit_lot_number": "sequencing_kit_lot_number",
            "cell_lot_number": "cell_lot_number",
            "include_kinetics": "include_kinetics",
            "loading_conc": "loading_conc",
            "control_num_reads": "control_num_reads",
            "control_read_length_mean": "control_read_length_mean",
            "control_concordance_mean": "control_concordance_mean",
            "control_concordance_mode": "control_concordance_mode",
            "local_base_rate": "local_base_rate",
            "polymerase_read_bases": "polymerase_read_bases",
            "polymerase_num_reads": "polymerase_num_reads",
            "polymerase_read_length_mean": "polymerase_read_length_mean",
            "polymerase_read_length_n50": "polymerase_read_length_n50",
            "insert_length_mean": "insert_length_mean",
            "insert_length_n50": "insert_length_n50",
            "unique_molecular_bases": "unique_molecular_bases",
            "productive_zmws_num": "productive_zmws_num",
            "p0_num": "p0_num",
            "p1_num": "p1_num",
            "p2_num": "p2_num",
            "adapter_dimer_percent": "adapter_dimer_percent",
            "short_insert_percent": "short_insert_percent",
            "hifi_read_bases": "hifi_read_bases",
            "hifi_num_reads": "hifi_num_reads",
            "hifi_read_length_mean": "hifi_read_length_mean",
            "hifi_read_quality_median": "hifi_read_quality_median",
            "hifi_number_passes_mean": "hifi_number_passes_mean",
            "hifi_low_quality_read_bases": "hifi_low_quality_read_bases",
            "hifi_low_quality_num_reads": "hifi_low_quality_num_reads",
            "hifi_low_quality_read_length_mean": "hifi_low_quality_read_length_mean",
            "hifi_low_quality_read_quality_median": "hifi_low_quality_read_quality_median",
            "hifi_barcoded_reads": "hifi_barcoded_reads",
            "hifi_bases_in_barcoded_reads": "hifi_bases_in_barcoded_reads",
        },
        "platform": {
            # platform.id is an auto-incremented integer, so we don't know what it is
            "run_id": "run.id",
            "platform_type": "name",
            "instrument_model": "model",
        },
        "run": {
            "run_id": "run.id",
            "instrument_name": "instrument_name",
            "lims_run_id": "lims_id",
            "element": "element",
            "run_start": "start",
            "run_complete": "complete",
            "plex_count": "plex_count",
        },
        "sample": {
            "sample_name": "sample.id",
            "tol_specimen_id": "specimen.id",  # Added by hand
            "biosample_accession": "accession_id",
        },
        "species": {
            "scientific_name": "species.id",
            "taxon_id": "taxon_id",
        },
        "specimen": {
            "tol_specimen_id": "specimen.id",
            "scientific_name": "species.id",  # Added by hand
            "biospecimen_accession": "accession_id",
        },
    }

    return table_map


@cache
def get_table_patcher(table):
    if col_map := table_map().get(table):
        patch_map = {}
        primary_key = None
        for key, out_key in col_map.items():
            if out_key == table + ".id":
                primary_key = key
            else:
                patch_map[key] = out_key
        if not primary_key:
            msg = f"Failed to find primary key in: {col_map}"
            raise ValueError(msg)

        def patcher(diff_list):
            patches_by_pk = {}
            for mm in diff_list:
                mlwh = mm.mlwh
                tolqc = mm.tolqc
                patch = {}
                for key, out_key in patch_map.items():
                    if tolqc[key] != mlwh[key]:
                        patch[out_key] = mlwh[key]
                if patch:
                    pk = mlwh[primary_key]
                    patch[table + ".id"] = pk
                    if have_patch := patches_by_pk.get(pk):
                        if patch != have_patch:
                            msg = (
                                f"Found two differing patches for {table}.id"
                                f" ('{primary_key}') '{pk}':\n"
                                f"{have_patch}\nand:\n{patch}\n"
                            )
                            raise ValueError(msg)
                    else:
                        patches_by_pk[pk] = patch
            return list(patches_by_pk.values())

        return patcher
    elif table == "accession":
        acc_types = {
            "biosample_accession": "BioSample",
            "biospecimen_accession": "BioSpecimen",
        }

        def patch_mlwh_accessions(diff_list):
            acc_patch = {}
            for mm in diff_list:
                mlwh = mm.mlwh
                tolqc = mm.tolqc
                for key, acc_type in acc_types.items():
                    old_acc = tolqc[key]
                    new_acc = mlwh[key]
                    if new_acc != old_acc:
                        new_acc = mlwh[key]
                        acc_patch[new_acc] = {
                            "accession.id": new_acc,
                            "accession_type_id": acc_type,
                        }
            return list(acc_patch.values())

        return patch_mlwh_accessions


COL_DEFS = {
    "data_id": "VARCHAR",
    "study_id": "INTEGER",
    "sample_name": "VARCHAR",
    "tol_specimen_id": "VARCHAR",
    "biosample_accession": "VARCHAR",
    "biospecimen_accession": "VARCHAR",
    "scientific_name": "VARCHAR",
    "taxon_id": "INTEGER",
    "platform_type": "VARCHAR",
    "instrument_model": "VARCHAR",
    "instrument_name": "VARCHAR",
    "pipeline_id_lims": "VARCHAR",
    "run_id": "VARCHAR",
    "lims_run_id": "VARCHAR",
    "element": "VARCHAR",
    "run_start": "TIMESTAMPTZ",
    "run_complete": "TIMESTAMPTZ",
    "plex_count": "INTEGER",
    "lims_qc": "VARCHAR",
    "qc_date": "TIMESTAMPTZ",
    "tag1_id": "VARCHAR",
    "tag2_id": "VARCHAR",
    "library_id": "VARCHAR",
    "movie_minutes": "INTEGER",
    "binding_kit": "VARCHAR",
    "sequencing_kit": "VARCHAR",
    "sequencing_kit_lot_number": "VARCHAR",
    "cell_lot_number": "VARCHAR",
    "include_kinetics": "VARCHAR",
    "loading_conc": "FLOAT",
    "control_num_reads": "INTEGER",
    "control_read_length_mean": "FLOAT",
    "control_concordance_mean": "FLOAT",
    "control_concordance_mode": "FLOAT",
    "local_base_rate": "FLOAT",
    "polymerase_read_bases": "BIGINT",
    "polymerase_num_reads": "INTEGER",
    "polymerase_read_length_mean": "FLOAT",
    "polymerase_read_length_n50": "INTEGER",
    "insert_length_mean": "FLOAT",
    "insert_length_n50": "INTEGER",
    "unique_molecular_bases": "BIGINT",
    "productive_zmws_num": "INTEGER",
    "p0_num": "INTEGER",
    "p1_num": "INTEGER",
    "p2_num": "INTEGER",
    "adapter_dimer_percent": "FLOAT",
    "short_insert_percent": "FLOAT",
    "hifi_read_bases": "BIGINT",
    "hifi_num_reads": "INTEGER",
    "hifi_read_length_mean": "INTEGER",
    "hifi_read_quality_median": "INTEGER",
    "hifi_number_passes_mean": "FLOAT",
    "hifi_low_quality_read_bases": "BIGINT",
    "hifi_low_quality_num_reads": "INTEGER",
    "hifi_low_quality_read_length_mean": "INTEGER",
    "hifi_low_quality_read_quality_median": "INTEGER",
    "hifi_barcoded_reads": "INTEGER",
    "hifi_bases_in_barcoded_reads": "BIGINT",
    "remote_path": "VARCHAR",
}


def table_cols():
    return "\n, ".join(
        f"{n} {t} PRIMARY KEY" if n == "data_id" else f"{n} {t}"
        for n, t in COL_DEFS.items()
    )


def json_cols():
    return "\n, ".join(f"{n}: '{t}'" for n, t in COL_DEFS.items())
