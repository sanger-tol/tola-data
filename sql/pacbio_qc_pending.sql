SELECT study.id_study_lims AS study_id
  , sample.name AS sample_name
  , sample.supplier_name AS supplier_name
  , sample.public_name AS tol_specimen_id
  , sample.accession_number AS biosample_accession
  , sample.donor_id AS biospecimen_accession
  , sample.common_name AS scientific_name
  , sample.taxon_id AS taxon_id
  , well_metrics.instrument_type AS instrument_model
  , run.pipeline_id_lims AS pipeline_id_lims
  , well_metrics.movie_name AS movie_name
  , well_metrics.pac_bio_run_name AS lims_run_id
  , well_metrics.well_label AS well_label
  , well_metrics.run_start AS run_start
  , well_metrics.run_complete AS run_complete
  , well_metrics.run_status AS run_status
  , IF(well_metrics.qc_seq IS NULL
      , NULL
      , IF(well_metrics.qc_seq = 1, 'pass'
          , 'fail')) AS lims_qc
  , well_metrics.qc_seq_date AS qc_date
  , run.tag_identifier AS tag1_id
  , run.tag2_identifier AS tag2_id
  , run.pac_bio_library_tube_name AS library_id
  , irods.irods_root_collection AS irods_path
  , irods.irods_data_relative_path AS irods_file
FROM sample
JOIN pac_bio_run AS run
  ON sample.id_sample_tmp = run.id_sample_tmp
JOIN pac_bio_product_metrics AS product_metrics
  ON run.id_pac_bio_tmp = product_metrics.id_pac_bio_tmp
JOIN pac_bio_run_well_metrics AS well_metrics
  ON product_metrics.id_pac_bio_rw_metrics_tmp = well_metrics.id_pac_bio_rw_metrics_tmp
JOIN study
  ON run.id_study_tmp = study.id_study_tmp
LEFT JOIN seq_product_irods_locations AS irods
  ON product_metrics.id_pac_bio_product = irods.id_product
WHERE product_metrics.qc IS NULL
  AND well_metrics.run_complete IS NOT NULL
  AND sample.taxon_id IS NOT NULL
  AND well_metrics.movie_name IS NOT NULL
  AND study.id_study_lims IN (
    3537, 3687, 3993, 4098, 4099, 4616, 4629, 4642, 5251, 4922, 5113, 5173,
    5632, 5308, 5732, 5733, 5822, 5884, 5881, 5853, 5901, 5740, 6101, 6327,
    6414, 6375, 6387, 6457, 6524, 5485, 6380, 6552, 6529, 6584, 6733, 6747,
    6830, 6313, 6322, 6859, 6923, 6921, 6920, 6919, 6899, 6898, 6893, 6772,
    6771, 6732, 6731, 6738, 6725, 6708, 6654, 6652, 6617, 6616, 6592, 6585,
    6583, 6969, 7112, 7107, 7106, 7104, 7103, 7102, 7101, 7005, 7007, 7233,
    7237, 7256 )
ORDER BY well_metrics.run_complete
  , sample.name
