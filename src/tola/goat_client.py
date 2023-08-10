import json
import logging
import re
import requests
import sys


class GoaTClient:
    def __init__(self):
        self.goat_url = "https://goat.genomehubs.org/api/v2"

    def json_get(self, payload):
        r = requests.get(f"{self.goat_url}/search", params=payload)
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
            setattr(self, name, val)

    def make_info(self):
        info = {
            "species_id": self.scientific_name,
            "hierarchy_name": self.hierarchy_name(),
            "strain": self.get_strain(),
            "common_name": self.get_name("common name"),
            "taxon_id": self.taxon_id,
            "taxon_family": self.get_lineage("family"),
            "taxon_order": self.get_lineage("order"),
            "taxon_phylum": self.get_lineage("phylum"),
            "taxon_group": self.get_taxon_group(),
            "genome_size": self.get_value("genome_size"),
            "chromosome_number": self.get_value("chromosome_number"),
        }
        return info

    def hierarchy_name(self):
        hn = re.sub(r"\W+", "_", self.scientific_name)
        return hn.strip("_")

    def get_strain(self):
        if self.taxon_rank == "subspecies":
            return self.scientific_name.split()[-1]
        elif m := re.search(
            r"\bstrain\s+(\S+)",
            self.scientific_name,
            re.IGNORECASE,
        ):
            return m.group(1)
        else:
            return None

    def get_name(self, name):
        """Returns the first instance found of 'name' argument"""
        for tn in self.taxon_names:
            if tn["class"] == name:
                return tn["name"]
        return None

    def get_lineage(self, name):
        for lg in self.lineage:
            if lg["taxon_rank"] == name:
                return lg["scientific_name"]
        return None

    def get_value(self, name):
        if fld := self.fields.get(name):
            return fld["value"]
        else:
            return None

    # Copied from tol-qc/asm2json which is based on:
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
        "t": "ctenophores",
        "u": "algae",
        "v": "vascular-plants",
        "w": "annelids",
        "x": "molluscs",
        "y": "bacteria",
        "z": "archaea",
    }

    def get_taxon_group(self):
        tol_id = self.get_name("tol_id")
        return self.LETTER_GROUP.get(tol_id[0]) if tol_id else None


def command_line_args(args=sys.argv[1:]):
    raw_flag = False
    tax_list = []
    for r in args:
        if re.match(r"-+raw", r):
            raw_flag = True
        elif re.fullmatch(r"\d+", r):
            tax_list.append(r)
        else:
            logging.warning(f"Unrecognised argument: '{r}'")
    return raw_flag, tax_list


if __name__ == "__main__":
    gc = GoaTClient()
    raw_flag, tax_list = command_line_args()
    if len(tax_list) == 0:
        tax_list = 13579, 116150, 348721, 2980486, 237398
    if raw_flag:
        for taxon_id in tax_list:
            print(json.dumps(gc.raw_results_from_taxon_id(taxon_id), indent=2))
    else:
        for taxon_id in tax_list:
            species_info = gc.get_species_info(taxon_id)
            print(json.dumps(species_info, indent=2))
