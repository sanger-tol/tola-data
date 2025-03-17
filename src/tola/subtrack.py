import logging
from functools import cache, cached_property
from inspect import cleandoc

from tola import db_connection


class SubTrack:
    def __init__(self, alias="subtrack", page_size=200):
        self.alias = alias
        self.page_size = page_size

    @cached_property
    def conn(self):
        return db_connection.make_connection(self.alias)

    def pages(self, book):
        page = self.page_size
        for i in range(0, len(book), page):
            yield book[i : i + page]

    SUB_INFO_DICT = {
        "files.file_name": "file_name",
        "sub.id": "id",
        "sub.run": "run",
        "sub.lane": "lane",
        "sub.mux": "tag",
        "stat.status": "status",
        "cv_status.description": "status_decription",
        "sub.study_id": "study_id",
        "sub.sample_id": "sample_id",
        "sub.ext_db": "archive",
        "sub.ebi_sub_acc": "submission_accession",
        "sub.ebi_study_acc": "data_accession",
        "sub.ebi_sample_acc": "sample_accession",
        "sub.ebi_exp_acc": "experiment_accession",
        "sub.ebi_run_acc": "run_accession",
        "rcpt.timestamp": "submission_time",
    }

    def fetch_submission_info(self, file_names):
        col_dict = self.SUB_INFO_DICT
        crsr = self.conn.cursor()
        col_names = list(col_dict.keys())
        for page in self.pages(file_names):
            sql = submission_info_sql(len(page))
            crsr.execute(sql, page)
            for row in crsr:
                yield {col_names[i]: val for i, val in enumerate(row)}


@cache
def submission_info_sql(count):
    placeholders = ",".join(["%s"] * count)
    select_cols = "\n          , ".join(
        f"{col} AS {name}" for col, name in SubTrack.SUB_INFO_DICT.items()
    )

    return cleandoc(
        f"""
        SELECT
            {select_cols}
        FROM submission sub
        JOIN sub_status stat
          ON stat.id = sub.id
          AND stat.is_current = 'Y'
        JOIN cv_status
          ON stat.status = cv_status.code
        JOIN files
          ON files.sub_id = sub.id
        LEFT JOIN receipt rcpt USING (ebi_sub_acc)
        WHERE files.file_name IN ({placeholders})
        """  # noqa: S608
    )
