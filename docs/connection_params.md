# `~/.connection_params.json` Configuration Files

The file `~/.connection_params.json` is used to store connection parameters
for network services.  It contains a single JSON object at the top level, the
keys of which are service names, and the values are another JSON object
containing key-value pairs for config parameters.  For the ToLQC database,
for example the default is to look for the service named `tolqc`.

It was created as an alternative to the `~/.netrc` file, to offer more
flexibility in the keys available.  Like `~/.netrc` it should be mode
`0600`, _i.e._ only readable by the user.

## Service Names

Uses lower case, hyphen-separated names which are easy to type on the command
line, _e.g._ for the `--tolqc-alias` command line option used by `tqc` and
related commands.

## Config Keys

These config keys have been used:

| Key       | Type    | Description                                          |
| --------- | ------- | ---------------------------------------------------- |
| api_token | string  | API access token                                     |
| api_url   | string  | Base URL of API                                      |
| database  | string  | Name of database                                     |
| dbd       | string  | Name of database dependent library to use to connect |
| host      | string  | Host name IP address or DNS name                     |
| password  | string  | Password for service                                 |
| port      | integer | Port for database connections                        |
| schema    | string  | Name of schema to connect to                         |
| service   | string  | Name of service to connect to                        |
| user      | string  | User name                                            |

The file format obvoiusly makes it easy to add more.

## Example

Example file contents for two services, `submissions` and `tolqc-test`:

```json
{
    "submissions": {
        "dbd": "mysql",
        "host": "subs-db.internal.sanger.ac.uk",
        "port": 3306,
        "database": "submissions",
        "user": "subs_ro",
        "password": null
    },
    "tolqc-test": {
        "api_url": "https://qc-test.tol.sanger.ac.uk",
        "api_token": "605520cb-ccbe-4819-9f59-105c8bfc0cb3"
    }
}
```

## Code

The [db_connection](../src/tola/db_connection.py) file contains code to return
config params for a service name, and which checks for the correct file
permissions.

