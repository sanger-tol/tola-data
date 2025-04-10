
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
  AND taxon_id = 13579;


-- Reformatted version of mlwh_datasource.py Illumina query.  Unnecessary
-- DISTINCT removed with join from product_metrics to run_land_metrics using
-- both id_run and position columns

SELECT sample.name AS sample_ref
  , sample.public_name AS public_name
  , sample.common_name AS common_name
  , sample.supplier_name AS supplier_name
  , sample.accession_number AS accession_number
  , sample.donor_id AS donor_id
  , sample.taxon_id AS taxon_id
  , sample.description AS description
  , run_lane_metrics.instrument_model AS instrument_model
  , CONVERT(run_lane_metrics.id_run, char) AS run_id
  , run_lane_metrics.run_pending AS start_date
  , run_lane_metrics.qc_complete AS qc_date
  , CONVERT(flowcell.position, char) AS position
  , CONVERT(flowcell.tag_index, char) AS tag_index
  , flowcell.pipeline_id_lims AS pipeline_id_lims
  , flowcell.tag_sequence AS tag_sequence
  , flowcell.tag2_sequence AS tag2_sequence
  , run_status_dict.description AS run_status
  , run_status.date AS complete_date
  , study.id_study_lims AS study_id
  , study.name AS study_name
  , flowcell.manual_qc AS manual_qc
  , 'iseq' AS platform_type
FROM sample
JOIN iseq_flowcell AS flowcell
  ON sample.id_sample_tmp = flowcell.id_sample_tmp
JOIN iseq_product_metrics AS product_metrics
  ON flowcell.id_iseq_flowcell_tmp = product_metrics.id_iseq_flowcell_tmp
JOIN iseq_run_lane_metrics AS run_lane_metrics
  ON product_metrics.id_run = run_lane_metrics.id_run
  AND product_metrics.position = run_lane_metrics.position
JOIN iseq_run_status AS run_status
  ON product_metrics.id_run = run_status.id_run
JOIN iseq_run_status_dict AS run_status_dict
  ON run_status.id_run_status_dict = run_status_dict.id_run_status_dict
JOIN study AS study
  ON flowcell.id_study_tmp = study.id_study_tmp
WHERE study.id_study_lims = 5901
  AND run_status.iscurrent = 1
  AND run_lane_metrics.qc_complete IS NOT NULL
ORDER BY run_lane_metrics.id_run


-- New Illumina query to use per-product QC outcomes

SELECT study.id_study_lims AS study_id
  , sample.name AS sample_name
  , sample.supplier_name AS supplier_name
  , sample.public_name AS tol_specimen_id
  , sample.accession_number AS biosample_accession
  , sample.donor_id AS biospecimen_accession
  , sample.taxon_id AS taxon_id
  , 'Illumina' AS platform_type
  , run_lane_metrics.instrument_model AS instrument_model
  , flowcell.pipeline_id_lims AS pipeline_id_lims
  , CONVERT(run_lane_metrics.id_run, char) AS run_id
  , run_lane_metrics.run_complete AS run_complete
  , CONCAT(run_lane_metrics.id_run, '_', flowcell.position, '#', flowcell.tag_index) AS name_root
  , IF(product_metrics.qc IS NULL, NULL, IF(product_metrics.qc = 1, 'pass', 'fail')) AS lims_qc
  , run_lane_metrics.qc_complete AS qc_date
  , CONVERT(flowcell.tag_index, char) AS tag_index
  , flowcell.tag_identifier AS tag1_id
  , flowcell.tag2_identifier AS tag2_id
  , flowcell.id_library_lims AS library_id
  , irods.irods_root_collection AS irods_path
  , irods.irods_data_relative_path AS irods_file
FROM sample
JOIN iseq_flowcell AS flowcell
  ON sample.id_sample_tmp = flowcell.id_sample_tmp
JOIN study
  ON flowcell.id_study_tmp = study.id_study_tmp
JOIN iseq_product_metrics AS product_metrics
  ON flowcell.id_iseq_flowcell_tmp = product_metrics.id_iseq_flowcell_tmp
JOIN iseq_run_lane_metrics AS run_lane_metrics
  ON product_metrics.id_run = run_lane_metrics.id_run
  AND product_metrics.position = run_lane_metrics.position
LEFT JOIN seq_product_irods_locations irods
  ON product_metrics.id_iseq_product = irods.id_product
WHERE run_lane_metrics.qc_complete IS NOT NULL
  AND study.id_study_lims = 5901;


-- New query for PacBio data

SELECT study.id_study_lims AS study_id
  , sample.name AS sample_name
  , sample.supplier_name AS supplier_name
  , sample.public_name AS tol_specimen_id
  , sample.accession_number AS biosample_accession
  , sample.donor_id AS biospecimen_accession
  , sample.taxon_id AS taxon_id
  , 'PacBio' AS platform_type
  , well_metrics.instrument_type AS instrument_model
  , run.pipeline_id_lims AS pipeline_id_lims
  , well_metrics.movie_name AS run_id
  , well_metrics.run_complete AS run_complete
  , CONCAT(well_metrics.movie_name, "#", run.tag_identifier
      , IF(run.tag2_identifier IS NOT NULL
          , CONCAT('#', run.tag2_identifier), '')) AS name_root
  , IF(well_metrics.qc_seq IS NULL, NULL
    , IF(well_metrics.qc_seq = 1, 'pass', 'fail')) AS lims_qc
  , well_metrics.qc_seq_date AS qc_date
  , run.well_label AS tag_index
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
WHERE product_metrics.qc IS NOT NULL
  AND study.id_study_lims = 5901



-- Query joining PacBio runs into iRODS locations table

 --       sample_ref  DTOL9702654
 --      public_name  lpJunEffu1
 --      common_name  Juncus effusus
 --    supplier_name  KDTOL10021
 -- accession_number  SAMEA7521953
 --         donor_id  SAMEA7521930
 --         taxon_id  13579
 --      description
 --    platform_type  Illumina
 -- instrument_model  NovaSeq
 --           run_id  36691
 --          qc_date  2021-03-18 12:16:43
 --          lims_qc  pass
 --         position  2
 --        tag_index  7
 -- pipeline_id_lims  Chromium genome
 --     tag_sequence  ATACCCAA
 --    tag2_sequence
 --         study_id  5901
 --       study_name  DTOL_Darwin Tree of Life
 --       irods_path  /seq/illumina/runs/36/36691/lane2/plex7
 --       irods_file  36691_2#7.cram


 --         study_id  5901
 --       study_name  DTOL_Darwin Tree of Life
 --       sample_ref  DTOL9838614
 --    supplier_name  FD21271880
 -- accession_number  SAMEA7521953
 --      public_name  lpJunEffu1
 --         donor_id  SAMEA7521930
 --         taxon_id  13579
 --      common_name  Juncus effusus
 --      description
 --           run_id  79924
 --        tag_index  1019
 --    tag1_sequence  ACACACTCTATCAGATT
 --    tag2_sequence
 --         position  D1
 --    plate_barcode  DN776795M
 -- pipeline_id_lims
 --    platform_type  PacBio
 -- instrument_model  Sequel2e
 --          qc_date  2021-02-22 23:07:55
 --           p1_num  4559996
 --            movie  m64097e_210221_172213
 --            yield  22632028638
 --       irods_path
 --       irods_file


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
  AND taxon_id = 13579;


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
WHERE y.id_study_lims = 5901;


-- When did samples arrive in SciOps?
-- Connects studies to samples before sequencing data is present.

SELECT study.id_study_lims AS study
  , sample.name AS sample_name
  , stock_resource.created AS sample_received
FROM study
JOIN stock_resource USING (id_study_tmp)
JOIN sample USING (id_sample_tmp)
WHERE study.id_study_lims = '5901';

