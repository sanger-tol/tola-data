import inspect
from sqlalchemy import text
from tola import db_connection


def main():
    engine, _ = db_connection.tola_db_engine()
    header = (
        "Group",
        "Specimen",
        "Date",
        "Run",
        "Movie Name",
        "Well",
        "Tag",
        "Sample Accession",
        "Run Accession",
        "Yield",
        "N50",
        "Species",
    )
    print("\t".join(header))

    with engine.connect() as connection:
        result = connection.execute(pacbio_table_sql())
        for (
            proj,
            taxon_group,
            specimen,
            date,
            run,
            movie_name,
            well,
            tag,
            sample_acc,
            run_acc,
            yld,
            n50,
            species,
        ) in result:
            if "{}" in proj:
                group = proj.format(taxon_group)
            else:
                group = proj
            print(
                tsv_row(
                    group,
                    specimen,
                    date.date().isoformat(),
                    run,
                    movie_name,
                    well,
                    tag,
                    sample_acc,
                    run_acc,
                    yld,
                    n50,
                    species,
                )
            )


def tsv_row(*args):
    strings = ("" if x is None else str(x) for x in args)
    return "\t".join(strings)


def pacbio_table_sql():
    return text(
        inspect.cleandoc(
            """
            SELECT project.hierarchy_name proj
              , species.taxon_group
              , specimen.specimen_id
              , data.date
              , run.lims_id run
              , run.run_id movie_name
              , run.element well
              , data.tag1_id tag
              , sample.accession_id sample_accession
              , data.accession_id run_accession
              , metrics.hifi_read_bases yield
              , metrics.insert_length_n50 n50
              , species.species_id species
            FROM species
            JOIN specimen
              ON species.species_id = specimen.species_id
            JOIN sample
              ON specimen.specimen_id = sample.specimen_id
            JOIN data
              ON sample.sample_id = data.sample_id
            JOIN library
              ON data.library_id = library.library_id
            JOIN run
              ON data.run_id = run.run_id
            JOIN platform
              ON run.platform_id = platform.id
            JOIN pacbio_run_metrics metrics
              ON run.run_id = metrics.run_id
            JOIN allocation
              ON data.data_id = allocation.data_id
            JOIN project
              ON allocation.project_id = project.project_id
            WHERE platform.name = 'PacBio'
            ORDER BY data.date DESC
              , specimen.specimen_id ASC
            """
        )
    )


if __name__ == "__main__":
    main()
