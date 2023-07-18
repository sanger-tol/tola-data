import json
import re
import requests
import sys


class GoatClient:
    def __init__(self):
        self.goat_url = "https://goat.genomehubs.org/api/v2"

    def json_get(self, payload):
        r = requests.get(f"{self.goat_url}/search", params=payload)
        if r.status_code == requests.codes.ok:
            return r.json()
        else:
            r.raise_for_status()

    def one_result_or_none(self, query, fields=None):
        payload = {
            "result": "taxon",
            "includeEstimates": "true",
            "taxonomy": "ncbi",
            "query": query,
            # "offset": 0,
        }
        if fields:
            payload["fields"] = ",".join(fields)
        data = self.json_get(payload)
        results = data.get("results")
        if len(results) == 1:
            return GoatResult(results[0]["result"])
        else:
            return None

    def one_result(self, query, fields=None):
        if rslt := self.one_result_or_none(self, query, fields):
            return rslt
        else:
            msg = (
                f"Expecting unique result for query '{query}'"
                f" but found '{len(results)}'"
            )
            raise ValueError(msg)

    def get_species_info(self, taxon_id):
        rslt = self.one_result_or_none(
            f"tax_eq({taxon_id})",
            fields=("genome_size", "chromosome_number"),
        )
        return rslt.make_info() if rslt else None


class GoatResult:
    def __init__(self, args):
        for name, val in args.items():
            setattr(self, name, val)

    def make_info(self):
        info = {
            "species_id": self.scientific_name,
            # "hierarchy_name": re.sub(r"\s+", "_", self.get_lineage("species")),
            "hierarchy_name": re.sub(r"\s+", "_", self.scientific_name),
            "strain": self.get_strain(),
            "common_name": self.get_name("common name"),
            "taxon_id": self.taxon_id,
            "taxon_family": self.get_lineage("family"),
            "taxon_order": self.get_lineage("order"),
            "taxon_phylum": self.get_lineage("phylum"),
            # "taxon_group": None,
            "genome_size": self.get_value("genome_size"),
            "chromosome_number": self.get_value("chromosome_number"),
        }
        return info

    def get_strain(self):
        # How will "strain" appear in GOAT?
        if self.taxon_rank == "subspecies":
            return self.scientific_name.split()[-1]
        else:
            return None

    def get_name(self, name):
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


if __name__ == "__main__":
    gc = GoatClient()
    tax_list = sys.argv[1:]
    if len(tax_list) == 0:
        tax_list = 13579, 116150, 348721
    for taxon_id in tax_list:
        species_info = gc.get_species_info(taxon_id)
        print(json.dumps(species_info, indent=2))
