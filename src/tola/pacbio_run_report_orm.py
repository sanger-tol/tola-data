import inspect
import sys
from sqlalchemy import select, text
from sqlalchemy.orm import Bundle
from tola import db_connection
from tola.tolqc_schema import (
    Allocation,
    Data,
    File,
    PacbioRunMetrics,
    Platform,
    Project,
    Run,
    Sample,
    Species,
    Specimen,
)


def main():
    engine, Session = db_connection.tola_db_engine()
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

    with Session() as session:
        query = (
            select(
                ProjectGroupBundle(
                    "group",
                    Project.hierarchy_name,
                    Species.taxon_group,
                ),
                Specimen.specimen_id,
                IsoDayBundle("date", Data.date),
                Run.lims_id.label("run"),
                Run.run_id.label("movie_name"),
                Run.element.label("well"),
                Data.tag1_id.label("tag"),
                Sample.accession_id.label("sample_accession"),
                Data.accession_id.label("run_accession"),
                PacbioRunMetrics.hifi_read_bases.label("yield"),
                PacbioRunMetrics.insert_length_n50.label("n50"),
                Species.species_id.label("species"),
            )
            .select_from(Data)
            .outerjoin(File)
            .outerjoin(Sample)
            .outerjoin(Specimen)
            .outerjoin(Species)
            .join(Run)
            .join(Platform)
            .outerjoin(PacbioRunMetrics)
            .join(Allocation)
            .join(Project)
            .where(Platform.name == 'PacBio')
            .order_by(
                Data.date.desc(),
                Specimen.specimen_id,
            )
        )
        # print(query)

        for row in session.execute(query).all():
            print(tsv_row(row))


class ProjectGroupBundle(Bundle):
    def create_row_processor(self, query, getters, labels):
        """
        Combine the "proj" and "taxon_group" columns if the "proj" column
        contains "{}", else returns the "proj" itself.
        e.g. ("darwin/{}", "birds") becomes "darwin/birds"
        """

        get_proj, get_taxon_group = getters

        def processor(row):
            proj = get_proj(row)
            taxon_group = get_taxon_group(row)
            group = None
            if proj is not None:
                if "{}" in proj:
                    if taxon_group is not None:
                        group = proj.format(taxon_group)
                else:
                    group = proj
            return group

        return processor


class IsoDayBundle(Bundle):
    def create_row_processor(self, query, getters, labels):
        """
        Returns just the day portion of a datetime column
        in ISO 8601 format if it contains a value.
        """

        (get_datetime,) = getters

        def processor(row):
            dt = get_datetime(row)
            return dt.date().isoformat() if dt else None

        return processor


def tsv_row(row):
    strings = ("" if x is None else str(x) for x in row)
    return "\t".join(strings)


if __name__ == "__main__":
    main()
