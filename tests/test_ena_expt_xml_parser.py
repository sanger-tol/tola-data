from pathlib import Path

import pytest

from tola.ena.ena_expt_xml_parser import EnaExptXmlParser


@pytest.fixture(scope="session")
def ena_xml_dir(data_dir):
    return data_dir / "ena_xml"


# Parsing ENA experiment files from:
#
#    https://www.ebi.ac.uk/ena/browser/api/xml/{}
#
# mVulVul1 data, 3 types from data_submission table:
#
#    data_id                       experiment_accession_id
#
#    48728_7-8#1                   ERX12234895
#    49280_2#18                    ERX12864202
#    m84047_240223_152838_s3#2013  ERX12247583

expected = [
    [
        "ERX12234895",
        {
            "accession": "ERX12234895",
            "bioproject_accession": "PRJEB74590",
            "biosample_accession": "SAMEA113398901",
            "center_name": "WELLCOME SANGER INSTITUTE",
            "design_description": (
                "Illumina sequencing of sample accession SAMEA113398901 "
                "for study accession PRJEB74590.  This submission "
                "includes reads tagged with the sequence TCCTTAAT. "
                "Library was made using a Hi-C - Arima v2 kit with "
                "restriction enzyme motif ^GATC,G^ANTC,C^TNAG,T^TAA."
            ),
            "instrument_model": "Illumina NovaSeq X",
            "library_construction_protocol": "Hi-C - Arima v2",
            "library_selection": "Restriction Digest",
            "library_source": "GENOMIC",
            "library_strategy": "Hi-C",
            "platform": "ILLUMINA",
        },
    ],
    [
        "ERX12864202",
        {
            "accession": "ERX12864202",
            "bioproject_accession": "PRJEB74590",
            "biosample_accession": "SAMEA113398901",
            "center_name": "WELLCOME SANGER INSTITUTE",
            "design_description": (
                "Illumina sequencing of sample accession SAMEA113398901 "
                "for study accession PRJEB74590.  This submission "
                "includes reads tagged with the sequence GCTTGCAT."
            ),
            "instrument_model": "Illumina NovaSeq X",
            "library_construction_protocol": "RNA PolyA",
            "library_selection": "PolyA",
            "library_source": "TRANSCRIPTOMIC",
            "library_strategy": "RNA-Seq",
            "platform": "ILLUMINA",
        },
    ],
    [
        "ERX12247583",
        {
            "accession": "ERX12247583",
            "bioproject_accession": "PRJEB74590",
            "biosample_accession": "SAMEA113398901",
            "center_name": "WELLCOME SANGER INSTITUTE",
            "design_description": (
                "PacBio sequencing of library DTOL13966585, constructed "
                "from sample accession SAMEA113398901 for study "
                "accession PRJEB74590."
            ),
            "instrument_model": "Revio",
            "library_construction_protocol": "PacBio - HiFi",
            "library_selection": "RANDOM",
            "library_source": "GENOMIC",
            "library_strategy": "WGS",
            "platform": "PACBIO_SMRT",
        },
    ],
]


@pytest.mark.parametrize("expt_id,doc", expected)
def test_parse_ena_expt_file(ena_xml_dir: Path, expt_id, doc):
    file = ena_xml_dir / f"{expt_id}.xml"
    (expt,) = EnaExptXmlParser().parse_file(file)
    assert expt == doc


@pytest.mark.parametrize("expt_id,doc", expected)
def test_parse_ena_expt_string(ena_xml_dir: Path, expt_id, doc):
    file = ena_xml_dir / f"{expt_id}.xml"
    xml = file.read_text()
    (expt,) = EnaExptXmlParser().parse_string(xml)
    assert expt == doc
