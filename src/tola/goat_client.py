import json
import sys
from functools import cached_property

import click
import requests

from tola.ndjson import ndjson_row


class GoaTClient:
    def __init__(self):
        self.goat_url = "https://goat.genomehubs.org/api/v2"

    def json_get(self, payload):
        r = requests.get(f"{self.goat_url}/search", params=payload, timeout=10)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            r.raise_for_status()

    def raw_result_list(self, payload):
        data = self.json_get(payload)
        if results := data.get("results"):
            return [r["result"] for r in results]
        else:
            return []

    def one_result_or_none(self, payload):
        data = self.json_get(payload)
        results = data.get("results")
        if len(results) == 1:
            return GoaTResult(results[0]["result"])
        else:
            return None

    def one_result(self, payload):
        if rslt := self.one_result_or_none(payload):
            return rslt
        else:
            msg = f"No result for query '{payload['query']}'"
            raise ValueError(msg)

    def taxon_id_payload(
        self,
        taxon_id,
        fields=("genome_size", "chromosome_number"),
    ):
        return {
            "query": f"tax_eq({taxon_id})",
            "fields": ",".join(fields),
            "result": "taxon",
            "taxonomy": "ncbi",
            "includeEstimates": "true",
        }

    def get_species_info(self, taxon_id):
        payload = self.taxon_id_payload(taxon_id)
        rslt = self.one_result_or_none(payload)
        return rslt.make_info() if rslt else None

    def raw_results_from_taxon_id(self, taxon_id):
        payload = self.taxon_id_payload(taxon_id)
        return self.raw_result_list(payload)


class GoaTResult:
    def __init__(self, args):
        for name, val in args.items():
            if name == "taxon_id":
                setattr(self, name, int(val))
            else:
                setattr(self, name, val)

    def make_info(self):
        info = {
            "species_id": self.scientific_name,
            "tolid_prefix": self.get_name("tolid prefix"),
            "common_name": self.get_name("common name"),
            "taxon_id": self.taxon_id,
            "family_taxon_id": self.get_lineage_taxon_id("family"),
            "taxon_family": self.get_lineage("family"),
            "taxon_order": self.get_lineage("order"),
            "taxon_phylum": self.get_lineage("phylum"),
            "taxon_group": self.get_taxon_group(),
            "genome_size": self.get_value("genome_size"),
            "chromosome_number": self.get_value("chromosome_number"),
        }
        return info

    def get_name(self, name):
        """Returns the first instance found of `name` argument"""
        for tn in self.taxon_names:
            if tn["class"] == name:
                return tn["name"]
        return None

    @cached_property
    def synonyms(self) -> set[str]:
        """
        Returns a set of the scientific name and all `equivalent name`,
        `synonym` and `includes` class taxon name entries.
        """
        synonyms = {self.scientific_name}
        for tn in self.taxon_names:
            if tn["class"] in {"equivalent name", "synonym", "includes"}:
                synonyms.add(tn["name"])
        return synonyms

    def get_lineage(self, name):
        for lg in self.lineage:
            if lg["taxon_rank"] == name:
                return lg["scientific_name"]
        return None

    def get_lineage_taxon_id(self, name):
        for lg in self.lineage:
            if lg["taxon_rank"] == name:
                return int(lg["taxon_id"])
        return None

    def get_value(self, name):
        if fld := self.fields.get(name):
            return fld["value"]
        else:
            return None

    # Sourced from:
    #   https://github.com/VGP/vgp-assembly/blob/master/VGP_specimen_naming_scheme.md
    LETTER_GROUP = {
        "a": "amphibians",
        "b": "birds",
        "c": "non-vascular-plants",
        "d": "dicots",
        "e": "echinoderms",
        "f": "fish",
        "g": "fungi",
        "h": "platyhelminths",
        "i": "insects",
        "j": "jellyfish",
        "k": "chordates",
        "l": "monocots",
        "m": "mammals",
        "n": "nematodes",
        "o": "sponges",
        "p": "protists",
        "q": "arthropods",
        "r": "reptiles",
        "s": "sharks",
        "t": "other-animal-phyla",
        "u": "algae",
        "v": "vascular-plants",
        "w": "annelids",
        "x": "molluscs",
        "y": "bacteria",
        "z": "archaea",
    }

    def get_taxon_group(self):
        tol_id = self.get_name("tolid prefix")
        return self.LETTER_GROUP.get(tol_id[0]) if tol_id else None


@click.command
@click.option(
    "--raw",
    "raw_flag",
    flag_value=True,
    default=False,
    show_default=True,
    help="""Print the full JSON reply from GoaT
      instead of ND-JSON suitable for feeding into `tqc`""",
)
@click.argument(
    "taxon_id_list",
    nargs=-1,
    required=True,
)
def cli(raw_flag, taxon_id_list):
    """Print species information from GoaT given a list of NCBI Taxon IDs,

    e.g.  goat-client 116150
    """

    gc = GoaTClient()
    if raw_flag:
        for taxon_id in taxon_id_list:
            print(json.dumps(gc.raw_results_from_taxon_id(taxon_id), indent=2))
    else:
        for taxon_id in taxon_id_list:
            if sp_info := gc.get_species_info(taxon_id):
                species_id = sp_info.pop("species_id")
                sys.stdout.write(ndjson_row({"species.id": species_id, **sp_info}))
            else:
                click.echo(f"No such Taxon ID {taxon_id}", err=True)


if __name__ == "__main__":
    cli()
