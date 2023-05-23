# SPDX-FileCopyrightText: 2023 Genome Research Ltd.
#
# SPDX-License-Identifier: MIT


from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    create_engine,
)
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

Base = declarative_base()
LogBase = Base
EnumBase = Base


def main():
    # inheritance(Assembly)
    engine = create_engine("sqlite:///assembly.sqlite", echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    ssn = Session()
    ssn.commit()


def test_creation_of_all_classes():
    for mapr in Base.registry.mappers:
        mapr_class = mapr.class_
        _ = mapr_class()


class Accession(LogBase):
    __tablename__ = "accession"

    class Meta:
        type_ = "accessions"

    accession_id = Column(String, primary_key=True)  # noqa: A003
    accession_type_id = Column(Integer, ForeignKey("accession_type_dict.id"))
    secondary = Column(String)
    submission = Column(String)
    date_submitted = Column(DateTime)
    title = Column(String)
    description = Column(String)

    accession_type_dict = relationship(
        "AccessionTypeDict",
        back_populates="accession",
        foreign_keys=[accession_type_id],
    )
    project = relationship("Project", back_populates="accession")
    specimen = relationship("Specimen", back_populates="accession")
    sample = relationship("Sample", back_populates="accession")
    data = relationship("Data", back_populates="accession")


class AccessionTypeDict(EnumBase):
    __tablename__ = "accession_type_dict"

    class Meta:
        type_ = "accession_types"

    accession = relationship("Accession", back_populates="accession_type_dict")


class Allocation(LogBase):
    __tablename__ = "allocation"

    class Meta:
        type_ = "allocations"

    id = Column(Integer, primary_key=True)  # noqa: A003
    project_id = Column(Integer, ForeignKey("project.id"))
    specimen_id = Column(Integer, ForeignKey("specimen.specimen_id"))
    is_primary = Column(Boolean)
    project = relationship(
        "Project", back_populates="allocations", foreign_keys=[project_id]
    )
    specimen = relationship(
        "Specimen", back_populates="allocations", foreign_keys=[specimen_id]
    )


class Assembly(LogBase):
    __tablename__ = "assembly"

    class Meta:
        type_ = "assemblies"

    id = Column(Integer, primary_key=True)  # noqa: A003
    dataset_id = Column(Integer, ForeignKey("dataset.id"))
    name = Column(String)
    description = Column(String)
    dataset = relationship(
        "Dataset", back_populates="assembly", foreign_keys=[dataset_id]
    )
    assembly_metrics = relationship("AssemblyMetrics", back_populates="assembly")
    busco_metrics = relationship("BuscoMetrics", back_populates="assembly")
    merqury_metrics = relationship("MerquryMetrics", back_populates="assembly")


class AssemblyComponent(LogBase, EnumBase):
    __tablename__ = "assembly_component"

    class Meta:
        type_ = "assembly_components"

    busco_metrics = relationship("BuscoMetrics", back_populates="assembly_component")
    merqury_metrics = relationship(
        "MerquryMetrics", back_populates="assembly_component"
    )
    assembly_metrics = relationship(
        "AssemblyMetrics", back_populates="assembly_component"
    )


class AssemblyMetrics(LogBase):
    __tablename__ = "assembly_metrics"

    class Meta:
        type_ = "assembly_metrics"

    id = Column(Integer, primary_key=True)  # noqa: A003
    assembly_id = Column(Integer, ForeignKey("assembly.id"))
    assembly_component_id = Column(Integer, ForeignKey("assembly_component.id"))
    bases = Column(Integer)
    a = Column(Integer)
    c = Column(Integer)
    g = Column(Integer)
    t = Column(Integer)
    n = Column(Integer)
    cpg = Column(Integer)
    iupac3 = Column(Integer)
    iupac2 = Column(Integer)
    ts = Column(Integer)
    tv = Column(Integer)
    cpg_ts = Column(Integer)
    contig_n = Column(Integer)
    contig_length = Column(Integer)
    contig_n50 = Column(Integer)
    contig_aun = Column(Float)
    contig_longest = Column(Integer)
    contig_shortest = Column(Integer)
    contig_length_mean = Column(Float)
    scaffold_n = Column(Integer)
    scaffold_n50 = Column(Integer)
    scaffold_aun = Column(Float)
    gap_n = Column(Integer)
    gap_n50 = Column(Integer)
    assembly = relationship(
        "Assembly", back_populates="assembly_metrics", foreign_keys=[assembly_id]
    )
    assembly_component = relationship(
        "AssemblyComponent",
        back_populates="assembly_metrics",
        foreign_keys=[assembly_component_id],
    )


class BuscoLineage(LogBase):
    __tablename__ = "busco_lineage"

    class Meta:
        type_ = "busco_lineages"

    id = Column(Integer, primary_key=True)  # noqa: A003
    name = Column(String)
    date_created = Column(DateTime)
    species_count = Column(Integer)
    gene_count = Column(Integer)
    busco_metrics = relationship("BuscoMetrics", back_populates="busco_lineage")


class BuscoMetrics(LogBase):
    __tablename__ = "busco_metrics"

    class Meta:
        type_ = "busco_metrics"

    id = Column(Integer, primary_key=True)  # noqa: A003
    assembly_id = Column(Integer, ForeignKey("assembly.id"))
    assembly_component_id = Column(Integer, ForeignKey("assembly_component.id"))
    complete = Column(Integer)
    single = Column(Integer)
    duplicated = Column(Integer)
    fragmented = Column(Integer)
    missing = Column(Integer)
    count = Column(Integer)
    busco_lineage_id = Column(Integer, ForeignKey("busco_lineage.id"))
    summary = Column(String)
    software_version_id = Column(Integer, ForeignKey("software_version.id"))
    assembly = relationship(
        "Assembly", back_populates="busco_metrics", foreign_keys=[assembly_id]
    )
    assembly_component = relationship(
        "AssemblyComponent",
        back_populates="busco_metrics",
        foreign_keys=[assembly_component_id],
    )
    busco_lineage = relationship(
        "BuscoLineage", back_populates="busco_metrics", foreign_keys=[busco_lineage_id]
    )
    software_version = relationship(
        "SoftwareVersion",
        back_populates="busco_metrics",
        foreign_keys=[software_version_id],
    )


class Centre(Base):
    __tablename__ = "centre"

    class Meta:
        type_ = "centres"

    id = Column(Integer, primary_key=True)  # noqa: A003
    name = Column(String)
    hierarchy_name = Column(String)
    run = relationship("Run", back_populates="centre")


class Data(LogBase):
    __tablename__ = "data"

    class Meta:
        type_ = "data"

    id = Column(Integer, primary_key=True)  # noqa: A003
    name = Column(String)
    hierarchy_name = Column(String)
    sample_id = Column(String, ForeignKey("sample.sample_id"))
    library_id = Column(Integer, ForeignKey("library.id"))
    accession_id = Column(String, ForeignKey("accession.accession_id"))
    run_id = Column(Integer, ForeignKey("run.id"))
    processed = Column(Integer)
    tag1_id = Column(String)
    tag2_id = Column(String)
    lims_qc = Column(Integer)
    auto_qc = Column(Integer)
    qc = Column(Integer)
    withdrawn = Column(Boolean)
    manually_withdrawn = Column(Boolean)
    sample = relationship("Sample", back_populates="data", foreign_keys=[sample_id])
    library = relationship("Library", back_populates="data", foreign_keys=[library_id])
    run = relationship("Run", back_populates="data", foreign_keys=[run_id])
    accession = relationship(
        "Accession", back_populates="data", foreign_keys=[accession_id]
    )
    set = relationship("Set", back_populates="data")  # noqa: A003
    file = relationship("File", back_populates="data")


class Dataset(LogBase):
    __tablename__ = "dataset"

    class Meta:
        type_ = "datasets"

    id = Column(Integer, primary_key=True)  # noqa: A003
    reads = Column(Integer)
    bases = Column(Integer)
    avg_read_len = Column(Float)
    read_len_n50 = Column(Float)
    set = relationship("Set", back_populates="dataset")  # noqa: A003
    merqury_metrics = relationship("MerquryMetrics", back_populates="dataset")
    assembly = relationship("Assembly", back_populates="dataset")
    genomescope_metrics = relationship("GenomescopeMetrics", back_populates="dataset")


class File(LogBase):
    __tablename__ = "file"

    class Meta:
        type_ = "files"

    id = Column(Integer, primary_key=True)  # noqa: A003
    data_id = Column(Integer, ForeignKey("data.id"))
    name = Column(String)
    type = Column(String)  # noqa: A003
    md5 = Column(String)
    data = relationship("Data", back_populates="file", foreign_keys=[data_id])


class GenomescopeMetrics(LogBase):
    __tablename__ = "genomescope_metrics"

    class Meta:
        type_ = "genomescope_metrics"

    id = Column(Integer, primary_key=True)  # noqa: A003
    dataset_id = Column(Integer, ForeignKey("dataset.id"))
    software_version_id = Column(Integer, ForeignKey("software_version.id"))
    review_id = Column(String, ForeignKey("review_dict.review_id"))
    kmer = Column(Integer)
    ploidy = Column(Integer)
    homozygous = Column(Float)
    heterozygous = Column(Float)
    haploid_length = Column(Integer)
    unique_length = Column(Integer)
    repeat_length = Column(Integer)
    kcov = Column(Float)
    kcov_init = Column(Integer)
    model_fit = Column(Float)
    read_error_rate = Column(Float)
    json = Column(String)

    dataset = relationship(
        "Dataset", back_populates="genomescope_metrics", foreign_keys=[dataset_id]
    )
    software_version = relationship(
        "SoftwareVersion",
        back_populates="genomescope_metrics",
        foreign_keys=[software_version_id],
    )
    review = relationship("ReviewDict", back_populates="genomescope_metrics")


class Library(LogBase):
    __tablename__ = "library"

    class Meta:
        type_ = "libraries"

    id = Column(Integer, primary_key=True)  # noqa: A003
    name = Column(String)
    hierarchy_name = Column(String)
    library_type_id = Column(Integer, ForeignKey("library_type.id"))
    lims_id = Column(Integer)
    data = relationship("Data", back_populates="library")
    library_type = relationship(
        "LibraryType", back_populates="library", foreign_keys=[library_type_id]
    )


class LibraryType(Base):
    __tablename__ = "library_type"

    class Meta:
        type_ = "library_types"

    id = Column(Integer, primary_key=True)  # noqa: A003
    name = Column(String)
    hierarchy_name = Column(String)
    kit = Column(String)
    enzyme = Column(String)
    library = relationship("Library", back_populates="library_type")


class MerquryMetrics(LogBase):
    __tablename__ = "merqury_metrics"

    class Meta:
        type_ = "merqury_metrics"

    id = Column(Integer, primary_key=True)  # noqa: A003
    assembly_id = Column(Integer, ForeignKey("assembly.id"))
    assembly_component_id = Column(Integer, ForeignKey("assembly_component.id"))
    dataset_id = Column(Integer, ForeignKey("dataset.id"))
    kmer = Column(String)
    complete_primary = Column(Integer)
    complete_alternate = Column(Integer)
    complete_all = Column(Integer)
    qv_primary = Column(Float)
    qv_alternate = Column(Float)
    qv_all = Column(Float)
    software_version_id = Column(Integer, ForeignKey("software_version.id"))
    assembly = relationship(
        "Assembly", back_populates="merqury_metrics", foreign_keys=[assembly_id]
    )
    dataset = relationship(
        "Dataset", back_populates="merqury_metrics", foreign_keys=[dataset_id]
    )
    assembly_component = relationship(
        "AssemblyComponent",
        back_populates="merqury_metrics",
        foreign_keys=[assembly_component_id],
    )
    software_version = relationship(
        "SoftwareVersion",
        back_populates="merqury_metrics",
        foreign_keys=[software_version_id],
    )


class PacbioRunMetrics(LogBase):
    __tablename__ = "pacbio_run_metrics"

    class Meta:
        type_ = "pacbio_run_metrics"

    id = Column(Integer, primary_key=True)  # noqa: A003
    run_id = Column(Integer, ForeignKey("run.id"))
    move_time = Column(Integer)
    pre_extension_time = Column(Integer)
    total_bases = Column(Integer)
    polymerase_reads = Column(Integer)
    polymerase_reads_bases = Column(Integer)
    polymerase_reads_mean = Column(Float)
    polymerase_reads_n50 = Column(Float)
    subreads_mean = Column(Float)
    subreads_n50 = Column(Float)
    insert_mean = Column(Float)
    insert_n50 = Column(Float)
    unique_molecular_bases = Column(Integer)
    p0 = Column(Integer)
    p1 = Column(Integer)
    p2 = Column(Integer)
    ccs_version_id = Column(String)
    ccs_pass = Column(Integer)
    ccs_fail = Column(Integer)
    demux_version_id = Column(String)
    demux_pass = Column(Integer)
    demux_fail = Column(Integer)
    run = relationship(
        "Run", back_populates="pacbio_run_metrics", foreign_keys=[run_id]
    )


class Platform(Base):
    __tablename__ = "platform"

    class Meta:
        type_ = "platforms"

    id = Column(Integer, primary_key=True)  # noqa: A003
    name = Column(String)
    model = Column(String)
    run = relationship("Run", back_populates="platform")


class Project(LogBase):
    __tablename__ = "project"

    class Meta:
        type_ = "projects"

    id = Column(Integer, primary_key=True)  # noqa: A003
    name = Column(String)
    hierarchy_name = Column(String)
    description = Column(String)
    lims_id = Column(Integer)
    accession_id = Column(String, ForeignKey("accession.accession_id"))
    accession = relationship(
        "Accession", back_populates="project", foreign_keys=[accession_id]
    )
    study = relationship("Study", back_populates="project")
    allocations = relationship("Allocation", back_populates="project")
    specimens = association_proxy("allocations", "specimen")


class QcDict(EnumBase):
    __tablename__ = "qc_dict"

    class Meta:
        type_ = "qc_types"

    status = relationship("Status", back_populates="qc_dict")
    genomescope_metrics = relationship("GenomescopeMetrics", back_populates="qc_dict")


class ReviewDict(LogBase):
    __tablename__ = "review_dict"

    class Meta:
        type_ = "review_dicts"
        id_column = "review_id"

    review_id = Column(String, primary_key=True)
    description = Column(String)

    genomescope_metrics = relationship("GenomescopeMetrics", back_populates="review")


class Run(LogBase):
    __tablename__ = "run"

    class Meta:
        type_ = "runs"

    id = Column(Integer, primary_key=True)  # noqa: A003
    name = Column(String)
    hierarchy_name = Column(String)
    platform_id = Column(Integer, ForeignKey("platform.id"))
    centre_id = Column(Integer, ForeignKey("centre.id"))
    lims_id = Column(Integer)
    element = Column(String)
    instrument_name = Column(String)
    date = Column(DateTime)
    data = relationship("Data", back_populates="run")
    platform = relationship(
        "Platform", back_populates="run", foreign_keys=[platform_id]
    )
    centre = relationship("Centre", back_populates="run", foreign_keys=[centre_id])
    pacbio_run_metrics = relationship("PacbioRunMetrics", back_populates="run")


class Sample(LogBase):
    __tablename__ = "sample"

    class Meta:
        type_ = "samples"
        id_column = "sample_id"

    sample_id = Column(String, primary_key=True)
    hierarchy_name = Column(String)
    lims_id = Column(Integer)
    specimen_id = Column(Integer, ForeignKey("specimen.specimen_id"))
    accession_id = Column(String, ForeignKey("accession.accession_id"))

    specimen = relationship(
        "Specimen", back_populates="sample", foreign_keys=[specimen_id]
    )
    accession = relationship(
        "Accession", back_populates="sample", foreign_keys=[accession_id]
    )
    data = relationship("Data", back_populates="sample")


class Set(LogBase):
    __tablename__ = "set"

    class Meta:
        type_ = "sets"

    id = Column(Integer, primary_key=True)  # noqa: A003
    data_id = Column(Integer, ForeignKey("data.id"))
    dataset_id = Column(Integer, ForeignKey("dataset.id"))
    data = relationship("Data", back_populates="set", foreign_keys=[data_id])
    dataset = relationship("Dataset", back_populates="set", foreign_keys=[dataset_id])


class Sex(LogBase):
    __tablename__ = "sex"

    class Meta:
        type_ = "sexes"
        id_column = "sex_id"

    sex_id = Column(String, primary_key=True)
    description = Column(String)

    # specimens = relationship("Specimen", back_populates="sex", foreign_keys=[sex_id])
    specimens = relationship("Specimen", back_populates="sex")


class SoftwareVersion(LogBase):
    __tablename__ = "software_version"

    class Meta:
        type_ = "software_versions"

    id = Column(Integer, primary_key=True)  # noqa: A003
    name = Column(String)
    version = Column(String)
    cmd = Column(String)
    busco_metrics = relationship("BuscoMetrics", back_populates="software_version")
    merqury_metrics = relationship("MerquryMetrics", back_populates="software_version")
    genomescope_metrics = relationship(
        "GenomescopeMetrics", back_populates="software_version"
    )


class Species(LogBase):
    __tablename__ = "species"

    class Meta:
        type_ = "species"
        id_column = "species_id"

    species_id = Column(String, primary_key=True)
    hierarchy_name = Column(String, nullable=False, unique=True)
    strain = Column(String)
    common_name = Column(String)
    taxon_id = Column(Integer)
    taxon_family = Column(String)
    taxon_order = Column(String)
    taxon_phylum = Column(String)
    taxon_group = Column(String)
    genome_size = Column(Integer)
    chromosome_number = Column(Integer)

    specimens = relationship("Specimen", back_populates="species")


class Specimen(LogBase):
    __tablename__ = "specimen"

    class Meta:
        type_ = "specimens"
        id_column = "specimen_id"

    specimen_id = Column(String, primary_key=True)
    hierarchy_name = Column(String, nullable=False, unique=True)
    specimen_status_id = Column(
        Integer, ForeignKey("specimen_status.specimen_status_id")
    )
    species_id = Column(Integer, ForeignKey("species.species_id"))
    lims_id = Column(Integer)
    supplied_name = Column(String)
    accession_id = Column(String, ForeignKey("accession.accession_id"))
    sex_id = Column(String, ForeignKey("sex.sex_id"))
    ploidy = Column(String)
    karyotype = Column(String)
    father_id = Column(Integer, ForeignKey("specimen.specimen_id"))
    mother_id = Column(Integer, ForeignKey("specimen.specimen_id"))

    species = relationship(
        "Species", back_populates="specimens", foreign_keys=[species_id]
    )
    father = relationship(
        "Specimen",
        primaryjoin="Specimen.father_id == Specimen.specimen_id",
        uselist=False,
    )
    mother = relationship(
        "Specimen",
        primaryjoin="Specimen.mother_id == Specimen.specimen_id",
        uselist=False,
    )
    sample = relationship("Sample", back_populates="specimen")
    status = relationship("SpecimenStatus", foreign_keys=[specimen_status_id])
    status_history = relationship(
        "SpecimenStatus",
        # foreign_keys=[specimen_id],
        primaryjoin="Specimen.specimen_id == SpecimenStatus.specimen_id",
        # back_populates="specimen",
    )

    # Removing foreign_keys=[sex_id] fixes error from SQLAlchemy:
    #
    #    sqlalchemy.exc.NoForeignKeysError: Could not determine join condition
    #    between parent/child tables on relationship Sex.specimens - there are
    #    no foreign keys linking these tables.  Ensure that referencing
    #    columns are associated with a ForeignKey or ForeignKeyConstraint, or
    #    specify a 'primaryjoin' expression.
    #
    # but produces this error from the API instead:
    #
    #    File "/Users/jgrg/git/tolqc/venv/lib/python3.10/site-packages/tol/api_base/model/base.py",
    #    line 669, in _get_type_from_tablename
    #
    #    return cls.tablename_type_dict[tablename]
    #    KeyError: 'specimens'

    # sex = relationship("Sex", back_populates="specimens", foreign_keys=[sex_id])
    sex = relationship("Sex", back_populates="specimens")

    accession = relationship(
        "Accession", back_populates="specimen", foreign_keys=[accession_id]
    )
    allocations = relationship("Allocation", back_populates="specimen")
    projects = association_proxy("allocations", "project")


class SpecimenStatus(LogBase):
    __tablename__ = "specimen_status"

    class Meta:
        type_ = "specimen_statuses"
        id_column = "specimen_status_id"

    specimen_status_id = Column(Integer, primary_key=True)
    specimen_id = Column(String, ForeignKey("specimen.specimen_id"))
    specimen_status_type_id = Column(
        String, ForeignKey("specimen_status_type.specimen_status_type_id")
    )
    status_time = Column(DateTime)

    specmien = relationship(
        "Specimen",
        foreign_keys=[specimen_id],
        back_populates="status_history",
    )
    status_type = relationship("SpecimenStatusType", back_populates="statuses")


class SpecimenStatusType(LogBase):
    __tablename__ = "specimen_status_type"

    class Meta:
        type_ = "specimen_status_types"
        id_column = "specimen_status_type_id"

    specimen_status_type_id = Column(String, primary_key=True)
    description = Column(String)
    assign_order = Column(Integer)

    statuses = relationship("SpecimenStatus", back_populates="status_type")


class Study(Base):
    __tablename__ = "study"

    class Meta:
        type_ = "studies"

    id = Column(Integer, primary_key=True)  # noqa: A003
    name = Column(String)
    lims_id = Column(Integer)
    project_id = Column(Integer, ForeignKey("project.id"))
    project = relationship("Project", back_populates="study", foreign_keys=[project_id])
