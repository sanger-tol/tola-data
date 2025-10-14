import sys
import xml.parsers.expat
from pathlib import Path


def main():
    files = sys.argv[1:]
    for file_name in files:
        file_path = Path(file_name)
        dlc = parse_dynamic_loading_cognate(file_path)
        print(f"{dlc}  {file_name}")


def parse_dynamic_loading_cognate(file: Path) -> bool | None:
    """
    Given a PacBio consensusreadset.xml file, returns the value (`True` or
    `False`) of the DynamicLoadingCognate automation paramter.

    Will return `None` if the tag is not present in the XML.

    Example element:

     <pbbase:AutomationParameter
       CreatedAt="2024-09-30T07:30:49.294+00:00"
       ModifiedAt="0001-01-01T00:00:00"
       Name="DynamicLoadingCognate"
       SimpleValue="False"
       ValueDataType="String"
      />
    """

    parser = xml.parsers.expat.ParserCreate()
    dlc = None

    def get_dynamic_loading_cognate(name, attrs):
        nonlocal dlc
        if (
            name == "pbbase:AutomationParameter"
            and attrs.get("Name") == "DynamicLoadingCognate"
        ):
            match attrs.get("SimpleValue"):
                case "False":
                    dlc = False
                case "True":
                    dlc = True
                case _:
                    msg = f"Unexpected value for 'SimpleValue' in {attrs = }"
                    raise ValueError(msg)
            # Now we have the value, we unset the handler so that the parser
            # will skip to the end of the file:
            parser.StartElementHandler = None

    parser.StartElementHandler = get_dynamic_loading_cognate
    parser.ParseFile(file.open('rb'))

    return dlc


if __name__ == "__main__":
    main()
