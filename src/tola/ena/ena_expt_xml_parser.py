import sys
from collections.abc import Buffer
from pathlib import Path
from xml.parsers.expat import ParserCreate

from tola.ndjson import ndjson_row


class EnaExptXmlParser:
    """
    Parses values from ENA experiment XML documents
    """

    def __init__(self):
        self.expat = ep = ParserCreate()
        ep.StartElementHandler = self._handle_start_element
        ep.EndElementHandler = self._handle_end_element
        ep.CharacterDataHandler = self._handle_char_data

        # Storage for data from the parser
        self._char_data: str = ""
        self._documents: list[dict[str, str]] = []
        self._current_document = {}

        # Flags to store when the parser is within certain tags
        self._in_biosample = False
        self._in_study_ref = False
        self._in_platform = False

    def parse_file(self, file: Path) -> list[dict[str, str]]:
        """
        Parses an XML file (given as a `pathlib.Path`), returning a list of
        experiment documents found.
        """
        fh = file.open(mode="rb")
        self.expat.ParseFile(fh)
        return self._documents

    def parse_string(self, xml: str | Buffer) -> list[dict[str, str]]:
        """
        Parses an XML string, returning a list of experiment documents found.
        """
        self.expat.Parse(xml, True)
        return self._documents

    def _handle_start_element(self, name, attr):
        self._char_data = ""
        doc = self._current_document
        if name == "EXPERIMENT":
            for k in "accession", "center_name":
                doc[k] = attr.get(k)
        elif name == "STUDY_REF":
            self._in_study_ref = True
        elif name == "EXTERNAL_ID":
            if attr.get("namespace") == "BioSample":
                self._in_biosample = True
        elif name == "PLATFORM":
            self._in_platform = True

    def _handle_char_data(self, data):
        self._char_data += data

    def _handle_end_element(self, name):
        doc = self._current_document
        txt = self._char_data.strip()

        if name in {
            "DESIGN_DESCRIPTION",
            "LIBRARY_STRATEGY",
            "LIBRARY_SOURCE",
            "LIBRARY_SELECTION",
            "LIBRARY_CONSTRUCTION_PROTOCOL",
            "INSTRUMENT_MODEL",
        }:
            doc[name.lower()] = txt
        elif name == "SECONDARY_ID":
            if self._in_study_ref:
                doc["bioproject_accession"] = txt
        elif name == "STUDY_REF":
            self._in_study_ref = False
        elif name == "EXTERNAL_ID":
            if self._in_biosample:
                doc["biosample_accession"] = txt
                self._in_biosample = False
        elif name == "PLATFORM":
            self._in_platform = False
        elif self._in_platform:
            doc["platform"] = name
        elif name == "EXPERIMENT":
            self._documents.append(doc)
            self._current_document = {}


if __name__ == "__main__":
    for file in sys.argv[1:]:
        for row in EnaExptXmlParser().parse_file(Path(file)):
            sys.stdout.write(ndjson_row(row))
