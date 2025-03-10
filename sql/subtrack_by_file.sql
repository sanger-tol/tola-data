SELECT sub.id
  , sub.run
  , sub.lane
  , sub.mux AS tag
  , stat.status
  , cv_status.description AS status_decription
  , sub.study_id
  , sub.sample_id
  , sub.ext_db AS archive
  , sub.ebi_sub_acc AS submission_accession
  , sub.ebi_study_acc AS data_accession
  , sub.ebi_sample_acc AS sample_accession
  , sub.ebi_exp_acc AS experiment_accession
  , sub.ebi_run_acc AS run_accession
  , UNIX_TIMESTAMP(rcpt.timestamp) AS submission_time
FROM submission sub
JOIN sub_status stat
  ON stat.id = sub.id
  AND stat.is_current = 'Y'
JOIN cv_status
  ON stat.status = cv_status.code
JOIN files
  ON files.sub_id = sub.id
LEFT JOIN receipt rcpt USING (ebi_sub_acc)
WHERE files.file_name = '48358_3-4#5.cram';
