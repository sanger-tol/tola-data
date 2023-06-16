
-- Query using all the studies listed in tol_track.conf joining through
-- pac_bio_run to the sample table, and using the ** Juncus effusus ** taxon
-- ID 13579 to find all its PacBio runs. (It is in 5901)

SELECT y.id_study_lims
  , y.name
  , r.*
  , s.*
FROM study y
JOIN pac_bio_run r
  ON y.id_study_tmp = r.id_study_tmp
JOIN sample s
  ON r.id_sample_tmp = s.id_sample_tmp
WHERE y.id_study_lims IN (
  3537, 3687, 3993, 4098, 4099, 4616, 4629, 4642, 5251, 4922, 5113, 5173,
  5632, 5308, 5732, 5733, 5822, 5884, 5881, 5853, 5901, 5740, 6101, 6327,
  6414, 6375, 6387, 6457, 6524, 5485, 6380, 6552, 6529, 6584, 6733, 6747,
  6830, 6313, 6322, 6859, 6923, 6921, 6920, 6919, 6899, 6898, 6893, 6772,
  6771, 6732, 6731, 6738, 6725, 6708, 6654, 6652, 6617, 6616, 6592, 6585,
  6969, 7112, 7107, 7106, 7104, 7103, 7102, 7101, 7005, 7007, 7233, 7237,
  7256
  )
  AND taxon_id = 13579


-- Query joining PacBio runs into iRODS locations table

SELECT y.id_study_lims
  , y.name
  , s.name
  , s.public_name
  , s.common_name
  , s.description
  , s.supplier_name
  , s.accession_number
  , r.well_label
  , r.tag_identifier
  , r.tag_sequence
  , r.tag2_sequence
  , r.pipeline_id_lims
  , pbrwm.instrument_type
  , pbrwm.well_label
  , loc.*
FROM study y
JOIN pac_bio_run r
  ON y.id_study_tmp = r.id_study_tmp
JOIN pac_bio_product_metrics pm
  ON r.id_pac_bio_tmp = pm.id_pac_bio_tmp
JOIN pac_bio_run_well_metrics pbrwm
  ON pm.id_pac_bio_rw_metrics_tmp = pbrwm.id_pac_bio_rw_metrics_tmp
LEFT JOIN seq_product_irods_locations loc
  ON pbrwm.id_pac_bio_product = loc.id_product
JOIN sample s
  ON r.id_sample_tmp = s.id_sample_tmp
WHERE y.id_study_lims = 5901
  AND taxon_id = 13579


-- Query joining Illumina data into iRODS locations table

SELECT s.name
  , s.public_name
  , s.supplier_name
  , rlm.instrument_model
  , rlm.run_complete
  , loc.*
FROM sample s
JOIN iseq_flowcell f
  ON s.id_sample_tmp = f.id_sample_tmp
JOIN study y
  ON f.id_study_tmp = y.id_study_tmp
JOIN iseq_product_metrics m
  ON f.id_iseq_flowcell_tmp = m.id_iseq_flowcell_tmp
JOIN iseq_run_lane_metrics rlm
  ON m.id_run = rlm.id_run
  AND m.position = rlm.position
LEFT JOIN seq_product_irods_locations loc
  ON m.id_iseq_product = loc.id_product
WHERE y.id_study_lims = 5901

