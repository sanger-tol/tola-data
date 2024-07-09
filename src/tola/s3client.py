import os
import sys

import boto3

from configparser import ConfigParser
from pathlib import Path


class S3Client:
    __slots__ = (
        "host_base",
        "secret_key",
        "access_key",
        "_s3",
    )

    def __init__(self):
        """
        Fetches configuration from the `s3cmd` format conifig file given
        in the `TOLA_S3_CONFIG_FILE` environment variable or the default
        `~/.s3cfg_tola`
        """
        cfg_path = (
            Path(env_def)
            if (env_def := os.getenv("TOLA_S3_CONFIG_FILE"))
            else Path().home() / ".s3cfg_tola"
        )
        if not cfg_path.exists():
            sys.exit(f"Config file {cfg_path} does not exist")
        cfg = ConfigParser(interpolation=None)
        cfg.read(cfg_path)

        # Pull config parameters from [default] section of INI file
        for param in self.__slots__:
            if param.startswith("_"):
                setattr(self, param, None)
                continue
            if val := cfg.get("default", param):
                setattr(self, param, val)
            else:
                sys.exit(
                    f"Missing value for key '{param}'"
                    f" in [default] section of {cfg_path}"
                )

    @property
    def s3(self):
        s3 = self._s3
        if not s3:
            self._s3 = s3 = boto3.client(
                "s3",
                endpoint_url="https://" + self.host_base,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
            )
        return s3

    def list_buckets(self):
        response = self.s3.list_buckets()
        for item in response['Buckets']:
            print(item['CreationDate'], item['Name'])
