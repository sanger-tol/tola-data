import click
import logging
import tola.query_result_formaters
from sqlalchemy import select
from sqlalchemy.orm import Bundle
from tola import db_connection
from tola.tolqc_schema import (
    Allocation,
    Data,
    Library,
    PacbioRunMetrics,
    Platform,
    Project,
    Run,
    Sample,
    Species,
    Specimen,
)


@click.command(help="Fetch PacBio run report from the ToL QC database")
@click.option(
    "--json",
    "format",
    flag_value="json",
    default=True,
    help="Print report in JSON format [default]",
)
@click.option(
    "--tsv",
    "format",
    flag_value="tsv",
    help="Print report in TSV format",
)
@db_connection.tolqc_db
def main(tolqc_db, format):
    engine, Session = tolqc_db

    with Session() as session:
        query = (
            select(
                ProjectGroupBundle(
                    "group",
                    Project.hierarchy_name,
                    Species.taxon_group,
                ),
                Specimen.specimen_id.label("specimen"),
                Library.library_type_id.label("pipeline"),
                Platform.name.label("platform"),
                Platform.model,
                Sample.sample_id.label("sanger_id"),
                IsoDayBundle("date", Run.start),
                Data.lims_qc,
                Run.lims_id.label("run"),
                Run.run_id.label("movie_name"),
                Run.element.label("well"),
                Run.instrument_name.label("instrument"),
                PacbioRunMetrics.movie_minutes.label("movie_length"),
                Data.tag_index,
                Data.tag1_id.label("tag"),
                Sample.accession_id.label("sample_accession"),
                Data.accession_id.label("run_accession"),
                Data.library_id.label("library_load_name"),
                PacbioRunMetrics.hifi_num_reads.label("reads"),
                PacbioRunMetrics.hifi_read_bases.label("bases"),
                PacbioRunMetrics.insert_length_mean.label("mean"),
                PacbioRunMetrics.insert_length_n50.label("n50"),
                Species.species_id.label("species"),
                PacbioRunMetrics.loading_conc,
                PacbioRunMetrics.binding_kit,
                PacbioRunMetrics.sequencing_kit,
                PacbioRunMetrics.include_kinetics,
            )
            .select_from(Data)
            .outerjoin(Sample)
            .outerjoin(Specimen)
            .outerjoin(Species)
            .join(Run)
            .join(Platform)
            .outerjoin(Library)
            .outerjoin(PacbioRunMetrics)
            # Cannot do many-to-many join between Data and Project directly.
            # Must explicitly go through Allocation:
            .join(Allocation)
            .join(Project)
            .where(Platform.name == "PacBio")
            .order_by(
                Data.date.desc(),
                Specimen.specimen_id,
            )
        )
        logging.debug(f"PacBio run report SQL: {query}")

        if format == "tsv":
            tola.query_result_formaters.output_tsv(session, query)
        elif format == "json":
            tola.query_result_formaters.output_json(session, query)


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
                if "{}" in proj and taxon_group is not None:
                    group = proj.format(taxon_group)
                else:
                    group = proj
            return group

        return processor


class IsoDayBundle(Bundle):
    def create_row_processor(self, query, getters, labels):
        """
        Returns just the day portion of a datetime column
        in ISO 8601 format, if it contains a value.
        """

        (get_datetime,) = getters

        def processor(row):
            dt = get_datetime(row)
            return dt.date().isoformat() if dt else None

        return processor
