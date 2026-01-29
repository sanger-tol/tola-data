import logging
from textwrap import dedent

from tola.fetch_mlwh_seq_data import formatted_response

log = logging.getLogger(__name__)


def test_response_formatting():
    rspns = response_data()
    fmttd = formatted_response(rspns, 5901, "PacBio")
    log.debug(fmttd)
    assert fmttd is not None
    assert fmttd == expected_formatted()


def expected_formatted():
    return dedent(
        """

        New PacBio data in 'DTOL_Darwin Tree of Life (5901)':

        m84047_240229_110633_s1\tdcVisVulg1\tViscaria vulgaris\tPacBio - HiFi\tDTOL14477585
        m84047_240229_113813_s2\tdaMyoScor1\tMyosotis scorpioides\tPacBio - HiFi\tDTOL14200424
        m84047_240229_121030_s3\twpNepCirr3\tNephtys cirrosa\tPacBio - HiFi\tDTOL14593466


        Updated PacBio data in 'DTOL_Darwin Tree of Life (5901)':

        m84093_240106_153052_s2\tdmAdoAnnu1\tAdonis annua\tPacBio - HiFi\tDTOL14161840
          date changed from None to '2024-03-06T14:19:38+00:00'
          lims_qc changed from None to 'pass'
        m84047_240214_124842_s4\tdaAdoMosc1\tAdoxa moschatellina\tPacBio - HiFi\tDTOL14550109
          date changed from '2024-02-20T15:05:57+00:00' to '2024-03-04T15:46:01+00:00'
          lims_qc changed from None to 'fail'
        """,
    )


def response_data():
    return {
        "new": [
            {
                "name": "m84047_240229_110633_s1",
                "specimen": "dcVisVulg1",
                "species": "Viscaria vulgaris",
                "library_type": "PacBio - HiFi",
                "sample": "DTOL14477585",
                "study": "DTOL_Darwin Tree of Life",
            },
            {
                "name": "m84047_240229_113813_s2",
                "specimen": "daMyoScor1",
                "species": "Myosotis scorpioides",
                "library_type": "PacBio - HiFi",
                "sample": "DTOL14200424",
                "study": "DTOL_Darwin Tree of Life",
            },
            {
                "name": "m84047_240229_121030_s3",
                "specimen": "wpNepCirr3",
                "species": "Nephtys cirrosa",
                "library_type": "PacBio - HiFi",
                "sample": "DTOL14593466",
                "study": "DTOL_Darwin Tree of Life",
            },
        ],
        "updated": [
            {
                "name": "m84093_240106_153052_s2",
                "specimen": "dmAdoAnnu1",
                "species": "Adonis annua",
                "library_type": "PacBio - HiFi",
                "sample": "DTOL14161840",
                "study": "DTOL_Darwin Tree of Life",
                "changes": {
                    "date": [None, "2024-03-06T14:19:38+00:00"],
                    "lims_qc": [None, "pass"],
                },
            },
            {
                "name": "m84047_240214_124842_s4",
                "specimen": "daAdoMosc1",
                "species": "Adoxa moschatellina",
                "library_type": "PacBio - HiFi",
                "sample": "DTOL14550109",
                "study": "DTOL_Darwin Tree of Life",
                "changes": {
                    "date": ["2024-02-20T15:05:57+00:00", "2024-03-04T15:46:01+00:00"],
                    "lims_qc": [None, "fail"],
                },
            },
        ],
    }
