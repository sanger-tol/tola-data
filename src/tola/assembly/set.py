from tola.tolqc_client import TolClient


class AssemblySetLoaderError(Exception):
    """
    Error from an AssemblySetLoader
    """


class AssemblySetLoader:
    def __init__(self, client: TolClient):
        self.client = client
