-- Using DuckDB to analyse input and output JSON data
-- from loading pacbio_run_metrics image Folders

CREATE TABLE store AS FROM 'pacbio_run_folders_stored.ndjson';
ALTER TABLE store RENAME COLUMN "pacbio_run_metrics.id" TO run_id;
ALTER TABLE store RENAME COLUMN "folder.id" TO folder_ulid;

CREATE TABLE dirs AS FROM 'pacbio_run_dirs.ndjson';
ALTER TABLE dirs RENAME COLUMN "pacbio_run_metrics.id" TO run_id;

CREATE TABLE files AS
  SELECT * EXCLUDE (image_file_list)
    , unnest(image_file_list, "recursive" := true)
  FROM store;

CREATE TABLE file_dict AS SELECT DISTINCT file FROM files ORDER BY ALL;

CREATE VIEW v_no_files AS
  SELECT run_id
    , directory
  FROM store
  JOIN dirs USING (run_id)
  ANTI JOIN files USING (run_id)
  ORDER BY ALL;

CREATE OR REPLACE VIEW v_file_folder_counts AS
  WITH folder_files AS (
      SELECT folder_ulid
        , array_agg(file ORDER BY file) file_list
      FROM files
      GROUP BY folder_ulid
  )
  SELECT count_star() n_folders
    , LENGTH(file_list) n_files
    , file_list
  FROM folder_files
  GROUP BY file_list
  ORDER BY n_folders DESC, n_files DESC, file_list;

CREATE OR REPLACE TABLE file_report AS
  WITH all_files AS (
    -- Cartesian join to list all possible
    -- folder / file combinations
    SELECT folder_ulid, file
    FROM store, file_dict
    ORDER BY ALL
  ),
  miss AS (
    SELECT *
    FROM all_files
    ANTI JOIN files
    USING (folder_ulid, file)
  ),
  missing_files AS (
    SELECT folder_ulid
      , array_agg(file ORDER BY file) miss_list
    FROM miss
    GROUP BY folder_ulid
  ),
  folder_files AS (
    SELECT folder_ulid
      , array_agg(file ORDER BY file) file_list
    FROM files
    GROUP BY folder_ulid
  )
  SELECT count_star() n_folders
    , len(file_list) n_files
    , file_list
    , miss_list
  FROM folder_files
  FULL JOIN missing_files USING (folder_ulid)
  GROUP BY file_list, miss_list
  ORDER BY n_folders DESC, n_files DESC, file_list;
