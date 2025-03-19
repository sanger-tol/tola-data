from functools import cached_property
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
        "submission.id": "id",
        "submission.run": "run",
        "submission.lane": "lane",
        "submission.mux": "tag",
        "sub_status.status": "status",
        "cv_status.description": "status_decription",
        "submission.study_id": "study_id",
        "submission.sample_id": "sample_id",
        "submission.ext_db": "archive",
        "submission.ebi_study_acc": "study_accession",
        "submission.ebi_sample_acc": "sample_accession",
        "submission.ebi_exp_acc": "experiment_accession",
        "submission.ebi_run_acc": "run_accession",
        "submission.ebi_sub_acc": "submission_accession",
        "receipt.timestamp": "submission_time",
    }

    def fetch_submission_info(self, file_names):
        crsr = self.conn.cursor()
        col_names = list(self.SUB_INFO_DICT.values())
        last_page = None
        for page in self.pages(file_names):
            page_size = len(page)
            if page_size != last_page:
                sql = self.submission_info_sql(page_size)
                last_page = page_size
            crsr.execute(sql, page)
            for row in crsr:
                yield {col_names[i]: val for i, val in enumerate(row)}

    def submission_info_sql(self, count):
        placeholders = ",".join(["%s"] * count)
        select_cols = "\n          , ".join(
            f"{col} AS {name}" for col, name in self.SUB_INFO_DICT.items()
        )

        return cleandoc(
            f"""
            SELECT
                {select_cols}
            FROM submission
            JOIN sub_status
              ON submission.id = sub_status.id
              AND sub_status.is_current = 'Y'
            JOIN cv_status
              ON sub_status.status = cv_status.code
            JOIN files
              ON submission.id = files.sub_id
            LEFT JOIN receipt USING (ebi_sub_acc)
            WHERE files.file_name IN ({placeholders})
            """  # noqa: S608
        )
