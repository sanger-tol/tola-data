import os
import sys
from configparser import ConfigParser
from functools import cached_property
from pathlib import Path

import boto3


class S3ConfigError(Exception):
    """Error in the ~/.s3cfg_tola S3 config file"""


class S3Client:
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
            msg = f"Config file {cfg_path} does not exist"
            raise S3ConfigError(msg)
        cfg = ConfigParser(interpolation=None)
        cfg.read(cfg_path)

        # Pull config parameters from [default] section of INI file
        for param in (
            "host_base",
            "secret_key",
            "access_key",
        ):
            if val := cfg.get("default", param):
                setattr(self, param, val)
            else:
                sys.exit(
                    f"Missing value for key '{param}'"
                    f" in [default] section of {cfg_path}"
                )

    @cached_property
    def s3(self):
        return boto3.client(
            "s3",
            endpoint_url="https://" + self.host_base,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

    def list_buckets(self):
        response = self.s3.list_buckets()
        for item in response["Buckets"]:
            print(item["CreationDate"], item["Name"])

    def put_file(self, local, bucket, remote):
        return self.s3.put_object(
            ACL="public-read",  # Might be better to set a policy on the bucket?
            Body=local,
            Bucket=bucket,
            Key=remote,
        )

    def delete_files(self, bucket, file_list):
        return self.s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": x} for x in file_list]},
        )


"""
    response = self.s3.put_object(
        Body=str(file),
        Bucket='tola-test',
        Key="/".join((folder_location_id, folder_ulid, file.name))
    )
"""
