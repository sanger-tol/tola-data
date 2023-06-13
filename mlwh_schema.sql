

CREATE TABLE ar_internal_metadata (

  `key`       varchar(255)  NOT NULL,
  `value`     varchar(255)  DEFAULT NULL,
  created_at  datetime(6)   NOT NULL,
  updated_at  datetime(6)   NOT NULL,

  PRIMARY KEY (`key`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;



CREATE TABLE bmap_flowcell (

  id_bmap_flowcell_tmp  int(11)           NOT NULL AUTO_INCREMENT,
  last_updated          datetime          NOT NULL                  COMMENT 'Timestamp of last update',
  recorded_at           datetime          NOT NULL                  COMMENT 'Timestamp of warehouse update',
  id_sample_tmp         int(10) unsigned  NOT NULL                  COMMENT 'Sample id, see "sample.id_sample_tmp"',
  id_study_tmp          int(10) unsigned  NOT NULL                  COMMENT 'Study id, see "study.id_study_tmp"',
  experiment_name       varchar(255)      NOT NULL                  COMMENT 'The name of the experiment, eg. The lims generated run id',
  instrument_name       varchar(255)      NOT NULL                  COMMENT 'The name of the instrument on which the sample was run',
  enzyme_name           varchar(255)      NOT NULL                  COMMENT 'The name of the recognition enzyme used',
  chip_barcode          varchar(255)      NOT NULL                  COMMENT 'Manufacturer chip identifier',
  chip_serialnumber     varchar(16)       DEFAULT NULL              COMMENT 'Manufacturer chip identifier',
  position              int(10) unsigned  DEFAULT NULL              COMMENT 'Flowcell position',
  id_flowcell_lims      varchar(255)      NOT NULL                  COMMENT 'LIMs-specific flowcell id',
  id_library_lims       varchar(255)      DEFAULT NULL              COMMENT 'Earliest LIMs identifier associated with library creation',
  id_lims               varchar(10)       NOT NULL                  COMMENT 'LIM system identifier',

  PRIMARY KEY (id_bmap_flowcell_tmp),
  KEY index_bmap_flowcell_on_id_flowcell_lims (id_flowcell_lims),
  KEY index_bmap_flowcell_on_id_library_lims (id_library_lims),
  KEY fk_bmap_flowcell_to_sample (id_sample_tmp),
  KEY fk_bmap_flowcell_to_study (id_study_tmp),
  CONSTRAINT fk_bmap_flowcell_to_sample FOREIGN KEY (id_sample_tmp) REFERENCES sample (id_sample_tmp),
  CONSTRAINT fk_bmap_flowcell_to_study FOREIGN KEY (id_study_tmp) REFERENCES study (id_study_tmp)
) ENGINE=InnoDB AUTO_INCREMENT=70 DEFAULT CHARSET=latin1;



CREATE TABLE cgap_analyte (

  cgap_analyte_tmp  int(10) unsigned  NOT NULL AUTO_INCREMENT                  COMMENT 'Internal to this database id. Value can change.',
  cell_line_uuid    varchar(36)       NOT NULL,
  destination       varchar(32)       NOT NULL,
  jobs              varchar(64)       DEFAULT NULL,
  slot_uuid         varchar(36)       NOT NULL,
  release_date      timestamp         NOT NULL DEFAULT '0000-00-00 00:00:00',
  labware_barcode   varchar(20)       NOT NULL,
  passage_number    int(2)            DEFAULT NULL,
  cell_state        varchar(40)       NOT NULL,
  project           varchar(50)       DEFAULT NULL,

  PRIMARY KEY (cgap_analyte_tmp),
  UNIQUE KEY slot_uuid (slot_uuid),
  KEY cell_line_uuid (cell_line_uuid)
) ENGINE=InnoDB AUTO_INCREMENT=28170268 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



CREATE TABLE cgap_biomaterial (

  cgap_biomaterial_tmp    int(10) unsigned  NOT NULL AUTO_INCREMENT  COMMENT 'Internal to this database id. Value can change.',
  donor_uuid              varchar(36)       NOT NULL,
  donor_accession_number  varchar(38)       DEFAULT NULL,
  donor_name              varchar(64)       DEFAULT NULL,
  biomaterial_uuid        varchar(36)       NOT NULL,

  PRIMARY KEY (cgap_biomaterial_tmp),
  UNIQUE KEY biomaterial_uuid (biomaterial_uuid),
  KEY donor_uuid (donor_uuid)
) ENGINE=InnoDB AUTO_INCREMENT=30870871 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



CREATE TABLE cgap_conjured_labware (

  cgap_conjured_labware_tmp  int(10) unsigned  NOT NULL AUTO_INCREMENT                  COMMENT 'Internal to this database id. Value can change.',
  barcode                    varchar(32)       NOT NULL,
  cell_line_long_name        varchar(48)       NOT NULL,
  cell_line_uuid             varchar(38)       NOT NULL,
  passage_number             int(2)            NOT NULL,
  fate                       varchar(40)       DEFAULT NULL,
  conjure_date               timestamp         NOT NULL DEFAULT '0000-00-00 00:00:00',
  labware_state              varchar(20)       NOT NULL,
  project                    varchar(50)       DEFAULT NULL,
  slot_uuid                  varchar(36)       NOT NULL,

  PRIMARY KEY (cgap_conjured_labware_tmp),
  UNIQUE KEY slot_uuid (slot_uuid),
  KEY barcode (barcode),
  KEY cell_line_long_name (cell_line_long_name),
  KEY cell_line_uuid (cell_line_uuid),
  KEY conjure_date (conjure_date),
  KEY labware_state (labware_state),
  KEY project (project)
) ENGINE=InnoDB AUTO_INCREMENT=34632459 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



CREATE TABLE cgap_destruction (

  cgap_destruction_tmp  int(10) unsigned  NOT NULL AUTO_INCREMENT                  COMMENT 'Internal to this database. Value can change.',
  barcode               varchar(32)       NOT NULL,
  cell_line_long_name   varchar(48)       NOT NULL,
  project               varchar(50)       DEFAULT NULL,
  destroyed             timestamp         NOT NULL DEFAULT '0000-00-00 00:00:00',
  cell_state            varchar(40)       NOT NULL,

  PRIMARY KEY (cgap_destruction_tmp),
  KEY barcode (barcode),
  KEY cell_line_long_name (cell_line_long_name),
  KEY project (project),
  KEY destroyed (destroyed)
) ENGINE=InnoDB AUTO_INCREMENT=14595929 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



CREATE TABLE cgap_heron (

  cgap_heron_tmp            int(10) unsigned  NOT NULL AUTO_INCREMENT                  COMMENT 'Internal to this database id. Value can change.',
  container_barcode         varchar(32)       NOT NULL,
  tube_barcode              varchar(32)       DEFAULT NULL,
  supplier_sample_id        varchar(64)       NOT NULL,
  position                  varchar(8)        NOT NULL,
  sample_type               varchar(32)       NOT NULL,
  release_time              timestamp         NOT NULL DEFAULT '0000-00-00 00:00:00',
  study                     varchar(32)       NOT NULL,
  destination               varchar(32)       NOT NULL,
  wrangled                  timestamp         NULL DEFAULT NULL,
  sample_state              varchar(32)       NOT NULL,
  lysis_buffer              varchar(64)       DEFAULT NULL,
  priority                  tinyint(4)        DEFAULT NULL,
  sample_identifier         varchar(64)       DEFAULT NULL                             COMMENT 'The COG-UK barcode of a sample or the mixtio barcode of a control',
  control_type              enum('            Positive','Negative') DEFAULT NULL,
  control_accession_number  varchar(32)       DEFAULT NULL,

  PRIMARY KEY (cgap_heron_tmp),
  UNIQUE KEY cgap_heron_rack_and_position (container_barcode,position),
  UNIQUE KEY tube_barcode (tube_barcode),
  KEY cgap_heron_supplier_sample_id (supplier_sample_id),
  KEY cgap_heron_release_time (release_time),
  KEY cgap_heron_study (study),
  KEY cgap_heron_destination_wrangled (destination,wrangled),
  KEY cgap_heron_sample_identifier (sample_identifier)
) ENGINE=InnoDB AUTO_INCREMENT=17647 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



CREATE TABLE cgap_line_identifier (

  cgap_line_identifier_tmp  int(10) unsigned  NOT NULL AUTO_INCREMENT  COMMENT 'Internal to this database id. Value can change.',
  line_uuid                 varchar(36)       NOT NULL,
  friendly_name             varchar(48)       NOT NULL,
  accession_number          varchar(38)       DEFAULT NULL,
  direct_parent_uuid        varchar(36)       DEFAULT NULL,
  biomaterial_uuid          varchar(36)       NOT NULL,
  project                   varchar(50)       DEFAULT NULL,

  PRIMARY KEY (cgap_line_identifier_tmp),
  UNIQUE KEY line_uuid (line_uuid),
  KEY direct_parent_uuid (direct_parent_uuid),
  KEY biomaterial_uuid (biomaterial_uuid),
  KEY friendly_name (friendly_name)
) ENGINE=InnoDB AUTO_INCREMENT=49940812 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



CREATE TABLE cgap_organoids_conjured_labware (

  cgap_organoids_conjured_labware_tmp  int(10) unsigned  NOT NULL AUTO_INCREMENT                  COMMENT 'Internal to this database id. Value can change.',
  barcode                              varchar(20)       NOT NULL,
  cell_line_long_name                  varchar(48)       NOT NULL,
  cell_line_uuid                       varchar(38)       NOT NULL,
  passage_number                       int(2)            NOT NULL,
  fate                                 varchar(40)       DEFAULT NULL,
  conjure_date                         timestamp         NOT NULL DEFAULT '0000-00-00 00:00:00',
  labware_state                        varchar(20)       NOT NULL,

  PRIMARY KEY (cgap_organoids_conjured_labware_tmp),
  KEY barcode (barcode),
  KEY cell_line_long_name (cell_line_long_name),
  KEY cell_line_uuid (cell_line_uuid),
  KEY conjure_date (conjure_date),
  KEY labware_state (labware_state)
) ENGINE=InnoDB AUTO_INCREMENT=10374306 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



CREATE TABLE cgap_release (

  cgap_release_tmp     int(10) unsigned  NOT NULL AUTO_INCREMENT                  COMMENT 'Internal to this database id. Value can change.',
  barcode              varchar(20)       NOT NULL,
  cell_line_long_name  varchar(48)       NOT NULL,
  cell_line_uuid       varchar(38)       NOT NULL,
  goal                 varchar(64)       NOT NULL,
  jobs                 varchar(64)       NOT NULL,
  destination          varchar(64)       DEFAULT NULL,
  `user`               varchar(6)        NOT NULL,
  release_date         timestamp         NOT NULL DEFAULT '0000-00-00 00:00:00',
  cell_state           varchar(40)       NOT NULL,
  fate                 varchar(40)       DEFAULT NULL,
  passage_number       int(2)            NOT NULL,
  project              varchar(50)       DEFAULT NULL,

  PRIMARY KEY (cgap_release_tmp),
  KEY barcode (barcode),
  KEY cell_line_long_name (cell_line_long_name),
  KEY cell_line_uuid (cell_line_uuid),
  KEY project (project)
) ENGINE=InnoDB AUTO_INCREMENT=112163406 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



CREATE TABLE cgap_supplier_barcode (

  cgap_supplier_barcode_tmp  int(10) unsigned  NOT NULL AUTO_INCREMENT                  COMMENT 'Internal to this database id. Value can change.',
  biomaterial_uuid           varchar(36)       NOT NULL,
  supplier_barcode           varchar(20)       NOT NULL,
  `date`                     timestamp         NOT NULL DEFAULT '0000-00-00 00:00:00',

  PRIMARY KEY (cgap_supplier_barcode_tmp),
  UNIQUE KEY supplier_barcode (supplier_barcode),
  KEY biomaterial_uuid (biomaterial_uuid)
) ENGINE=InnoDB AUTO_INCREMENT=31756098 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;

 1 AS root_sample_id,
 1 AS plate_barcode,
 1 AS phenotype,
 1 AS coordinate,
 1 AS created,
 1 AS robot_type*/;
SET character_set_client = @saved_cs_client;


CREATE TABLE flgen_plate (

  id_flgen_plate_tmp   int(10) unsigned  NOT NULL AUTO_INCREMENT  COMMENT 'Internal to this database id, value can change',
  id_sample_tmp        int(10) unsigned  NOT NULL                 COMMENT 'Sample id, see "sample.id_sample_tmp"',
  id_study_tmp         int(10) unsigned  NOT NULL                 COMMENT 'Study id, see "study.id_study_tmp"',
  cost_code            varchar(20)       NOT NULL                 COMMENT 'Valid WTSI cost code',
  id_lims              varchar(10)       NOT NULL                 COMMENT 'LIM system identifier, e.g. CLARITY-GCLP, SEQSCAPE',
  last_updated         datetime          NOT NULL                 COMMENT 'Timestamp of last update',
  recorded_at          datetime          NOT NULL                 COMMENT 'Timestamp of warehouse update',
  plate_barcode        int(10) unsigned  NOT NULL                 COMMENT 'Manufacturer (Fluidigm) chip barcode',
  plate_barcode_lims   varchar(128)      DEFAULT NULL             COMMENT 'LIMs-specific plate barcode',
  plate_uuid_lims      varchar(36)       DEFAULT NULL             COMMENT 'LIMs-specific plate uuid',
  id_flgen_plate_lims  varchar(20)       NOT NULL                 COMMENT 'LIMs-specific plate id',
  plate_size           smallint(6)       DEFAULT NULL             COMMENT 'Total number of wells on a plate',
  plate_size_occupied  smallint(6)       DEFAULT NULL             COMMENT 'Number of occupied wells on a plate',
  well_label           varchar(10)       NOT NULL                 COMMENT 'Manufactuer well identifier within a plate, S001-S192',
  well_uuid_lims       varchar(36)       DEFAULT NULL             COMMENT 'LIMs-specific well uuid',
  qc_state             tinyint(1)        DEFAULT NULL             COMMENT 'QC state; 1 (pass), 0 (fail), NULL (not known)',

  PRIMARY KEY (id_flgen_plate_tmp),
  KEY flgen_plate_id_lims_id_flgen_plate_lims_index (id_lims,id_flgen_plate_lims),
  KEY flgen_plate_sample_fk (id_sample_tmp),
  KEY flgen_plate_study_fk (id_study_tmp),
  CONSTRAINT flgen_plate_sample_fk FOREIGN KEY (id_sample_tmp) REFERENCES sample (id_sample_tmp) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT flgen_plate_study_fk FOREIGN KEY (id_study_tmp) REFERENCES study (id_study_tmp) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=365864 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



CREATE TABLE gsu_sample_uploads (

  id_gsu_sample_upload_tmp  int(10) unsigned  NOT NULL AUTO_INCREMENT                                COMMENT 'Row ID',
  created                   datetime          DEFAULT CURRENT_TIMESTAMP                              COMMENT 'Datetime this record was created',
  last_changed              datetime          DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP  COMMENT 'Datetime this record was last updated',
  file_path                 varchar(255)      NOT NULL                                               COMMENT 'Location of data file',
  id_study_tmp              int(10) unsigned  NOT NULL                                               COMMENT 'Study for this item',
  id_sample_tmp             int(10) unsigned  NOT NULL                                               COMMENT 'Sample info for this item',
  library_name              varchar(40)       NOT NULL                                               COMMENT 'Supplier library name',
  library_type              varchar(40)       NOT NULL                                               COMMENT 'Library type',
  instrument_model          varchar(40)       NOT NULL                                               COMMENT 'Sequencing machine used',
  lab_name                  varchar(100)      NOT NULL                                               COMMENT 'Lab supplying the data',
  run_accession             varchar(40)       DEFAULT NULL                                           COMMENT 'ENA run accession, populated on ENA submission',

  PRIMARY KEY (id_gsu_sample_upload_tmp),
  UNIQUE KEY gsu_su_file_path_unq (file_path),
  UNIQUE KEY gsu_su_run_accession (run_accession),
  KEY gsu_su_study (id_study_tmp),
  KEY gsu_su_sample (id_sample_tmp),
  CONSTRAINT gsu_su_sample_fk FOREIGN KEY (id_sample_tmp) REFERENCES sample (id_sample_tmp),
  CONSTRAINT gsu_su_study_fk FOREIGN KEY (id_study_tmp) REFERENCES study (id_study_tmp)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



CREATE TABLE iseq_external_product_components (

  id_iseq_ext_pr_components_tmp  bigint(20) unsigned  NOT NULL AUTO_INCREMENT  COMMENT 'Internal to this database id, value can change',
  id_iseq_product_ext            char(64)             NOT NULL                 COMMENT 'id (digest) for the external product composition',
  id_iseq_product                char(64)             NOT NULL                 COMMENT 'id (digest) for one of the products components',
  num_components                 tinyint(3) unsigned  NOT NULL                 COMMENT 'Number of component products for this product',
  component_index                tinyint(3) unsigned  NOT NULL                 COMMENT 'Unique component index within all components of this product, a value from 1 to the value of num_components column for this product',

  PRIMARY KEY (id_iseq_ext_pr_components_tmp),
  UNIQUE KEY iseq_ext_pr_comp_unique (id_iseq_product,id_iseq_product_ext),
  KEY iseq_ext_pr_comp_pr_comp_fk (id_iseq_product_ext),
  KEY iseq_ext_pr_comp_ncomp (num_components,id_iseq_product),
  KEY iseq_ext_pr_comp_compi (component_index,num_components),
  CONSTRAINT id_iseq_product_ext_digest_fk FOREIGN KEY (id_iseq_product_ext) REFERENCES iseq_external_product_metrics (id_iseq_product) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=819527 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='Table linking iseq_external_product_metrics table products to components in the iseq_product_metrics table';



CREATE TABLE iseq_external_product_metrics (

  id_iseq_ext_pr_metrics_tmp                        bigint(20) unsigned  NOT NULL AUTO_INCREMENT                                COMMENT 'Internal to this database id, value can change',
  created                                           datetime             DEFAULT CURRENT_TIMESTAMP                              COMMENT 'Datetime this record was created',
  last_changed                                      datetime             DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP  COMMENT 'Datetime this record was created or changed',
  supplier_sample_name                              varchar(255)         CHARACTER SET utf8 DEFAULT NULL                        COMMENT 'Sample name given by the supplier, as recorded by WSI',
  plate_barcode                                     varchar(255)         CHARACTER SET utf8 DEFAULT NULL                        COMMENT 'Stock plate barcode, as recorded by WSI',
  library_id                                        int(11)              DEFAULT NULL                                           COMMENT 'WSI library identifier',
  file_name                                         varchar(300)         NOT NULL                                               COMMENT 'Comma-delimitered alphabetically sorted list of file names, which unambigiously define WSI sources of data',
  file_path                                         varchar(760)         NOT NULL                                               COMMENT 'Comma-delimitered alphabetically sorted list of full external file paths for the files in file_names column as uploaded by WSI',
  md5_staging                                       char(32)             DEFAULT NULL                                           COMMENT 'WSI validation hex MD5, not set for multiple source files',
  manifest_upload_status                            char(15)             DEFAULT NULL                                           COMMENT 'WSI manifest upload status, one of "IN PROGRESS", "DONE", "FAIL", not set for multiple source files',
  manifest_upload_status_change_date                datetime             DEFAULT NULL                                           COMMENT 'Date the status of manifest upload is changed by WSI',
  id_run                                            int(10) unsigned     DEFAULT NULL                                           COMMENT 'NPG run identifier, defined where the product corresponds to a single line',
  id_iseq_product                                   char(64)             CHARACTER SET utf8 DEFAULT NULL                        COMMENT 'product id',
  iseq_composition_tmp                              varchar(600)         DEFAULT NULL                                           COMMENT 'JSON representation of the composition object, the column might be deleted in future',
  id_archive_product                                char(64)             DEFAULT NULL                                           COMMENT 'Archive ID for data product',
  destination                                       varchar(15)          DEFAULT 'UKBMP'                                        COMMENT 'Data destination, from 20200323 defaults to "UKBMP"',
  processing_status                                 char(15)             DEFAULT NULL                                           COMMENT 'Overall status of the product, one of "PASS", "HOLD", "INSUFFICIENT", "FAIL"',
  qc_overall_assessment                             char(4)              DEFAULT NULL                                           COMMENT 'State of the product after phase 3 of processing, one of "PASS" or "FAIL"',
  qc_status                                         char(15)             DEFAULT NULL                                           COMMENT 'State of the product after phase 2 of processing, one of "PASS", "HOLD", "INSUFFICIENT", "FAIL"',
  sequencing_start_date                             date                 DEFAULT NULL                                           COMMENT 'Sequencing start date obtained from the CRAM file header, not set for multiple source files',
  upload_date                                       date                 DEFAULT NULL                                           COMMENT 'Upload date, not set for multiple source files',
  md5_validation_date                               date                 DEFAULT NULL                                           COMMENT 'Date of MD5 validation, not set for multiple source files',
  processing_start_date                             date                 DEFAULT NULL                                           COMMENT 'Processing start date',
  analysis_start_date                               date                 DEFAULT NULL,
  phase2_end_date                                   datetime             DEFAULT NULL                                           COMMENT 'Date the phase 2 analysis finished for this product',
  analysis_end_date                                 date                 DEFAULT NULL,
  archival_date                                     date                 DEFAULT NULL                                           COMMENT 'Date made available or pushed to archive service',
  archive_confirmation_date                         date                 DEFAULT NULL                                           COMMENT 'Date of confirmation of integrity of data product by archive service',
  md5                                               char(32)             DEFAULT NULL                                           COMMENT 'External validation hex MD5, not set for multiple source files',
  md5_validation                                    char(4)              DEFAULT NULL                                           COMMENT 'Outcome of MD5 validation as "PASS" or "FAIL", not set for multiple source files',
  format_validation                                 char(4)              DEFAULT NULL                                           COMMENT 'Outcome of format validation as "PASS" or "FAIL", not set for multiple source files',
  upload_status                                     char(4)              DEFAULT NULL                                           COMMENT 'Upload status as "PASS" or "FAIL", "PASS" if both MD5 and format validation are "PASS", not set for multiple source files',
  instrument_id                                     varchar(256)         DEFAULT NULL                                           COMMENT 'Comma separated sorted list of instrument IDs obtained from the CRAM file header(s)',
  flowcell_id                                       varchar(256)         DEFAULT NULL                                           COMMENT 'Comma separated sorted list of flowcell IDs obtained from the CRAM file header(s)',
  annotation                                        varchar(15)          DEFAULT NULL                                           COMMENT 'Annotation regarding data provenance, i.e. is sequence data from first pass, re-run, top-up, etc.',
  min_read_length                                   tinyint(3) unsigned  DEFAULT NULL                                           COMMENT 'Minimum read length observed in the data file',
  target_autosome_coverage_threshold                int(3) unsigned      DEFAULT '15'                                           COMMENT 'Target autosome coverage threshold, defaults to 15',
  target_autosome_gt_coverage_threshold             float                DEFAULT NULL                                           COMMENT 'Coverage percent at >= target_autosome_coverage_threshold X as a fraction',
  target_autosome_gt_coverage_threshold_assessment  char(4)              DEFAULT NULL                                           COMMENT '"PASS" if target_autosome_percent_gt_coverage_threshold > 95%, "FAIL" otherwise',
  verify_bam_id_score                               float unsigned       DEFAULT NULL                                           COMMENT 'FREEMIX value of sample contamination levels as a fraction',
  verify_bam_id_score_assessment                    char(4)              DEFAULT NULL                                           COMMENT '"PASS" if verify_bam_id_score > 0.01, "FAIL" otherwise',
  double_error_fraction                             float unsigned       DEFAULT NULL                                           COMMENT 'Fraction of marker pairs with two read pairs evidencing parity and non-parity, may only be calculated if 1% <= verify_bam_id_score < 5%',
  contamination_assessment                          char(4)              DEFAULT NULL                                           COMMENT '"PASS" or "FAIL" based on verify_bam_id_score_assessment and double_error_fraction < 0.2%',
  yield_whole_genome                                float unsigned       DEFAULT NULL                                           COMMENT 'Sequence data quantity (Gb) excluding duplicate reads, adaptors, overlapping bases from reads on the same fragment, soft-clipped bases',
  yield                                             float unsigned       DEFAULT NULL                                           COMMENT 'Sequence data quantity (Gb) excluding duplicate reads, adaptors, overlapping bases from reads on the same fragment, soft-clipped bases, non-N autosome only',
  yield_q20                                         bigint(20) unsigned  DEFAULT NULL                                           COMMENT 'Yield in bases at or above Q20 filtered in the same way as the yield column values',
  yield_q30                                         bigint(20) unsigned  DEFAULT NULL                                           COMMENT 'Yield in bases at or above Q30 filtered in the same way as the yield column values',
  num_reads                                         bigint(20) unsigned  DEFAULT NULL                                           COMMENT 'Number of reads filtered in the same way as the yield column values',
  gc_fraction_forward_read                          float unsigned       DEFAULT NULL,
  gc_fraction_reverse_read                          float unsigned       DEFAULT NULL,
  adapter_contamination                             varchar(255)         DEFAULT NULL                                           COMMENT 'The maximum over adapters and cycles in reads/fragments as a fraction per file and RG. Values for first and second reads separated with ",", and values for individual files separated with "/". e.g. "0.1/0.1/0.1/0.1,0.1/0.1/0.1/0.1"',
  adapter_contamination_assessment                  varchar(255)         DEFAULT NULL                                           COMMENT '"PASS", "WARN", "FAIL" per read and file. Multiple values are represented as forward slash-separated array of strings with a comma separating entries for paired-end 1 and 2 reads e.g. "PASS/PASS/WARN/PASS,PASS/PASS/WARN/PASS"',
  pre_adapter_min_total_qscore                      tinyint(3) unsigned  DEFAULT NULL                                           COMMENT 'Minimum of TOTAL_QSCORE values in PreAdapter report from CollectSequencingArtifactMetrics',
  ref_bias_min_total_qscore                         tinyint(3) unsigned  DEFAULT NULL                                           COMMENT 'Minimum of TOTAL_QSCORE values in BaitBias report from CollectSequencingArtifactMetrics',
  target_proper_pair_mapped_reads_fraction          float unsigned       DEFAULT NULL                                           COMMENT 'Fraction of properly paired mapped reads filtered in the same way as the yield column values',
  target_proper_pair_mapped_reads_assessment        char(4)              DEFAULT NULL                                           COMMENT '"PASS" if target_proper_pair_mapped_reads_fraction > 0.95, "FAIL" otherwise',
  insert_size_mean                                  float unsigned       DEFAULT NULL,
  insert_size_std                                   float unsigned       DEFAULT NULL,
  sequence_error_rate                               float unsigned       DEFAULT NULL                                           COMMENT 'Reported by samtools, as a fraction',
  basic_statistics_assessement                      varchar(255)         DEFAULT NULL                                           COMMENT 'FastQC "PASS", "WARN", "FAIL" per input file. Array of strings separated by "/", with a "," separating entries for paired-end 1 and 2 reads. e.g. Four RG "PASS/PASS/WARN/PASS,PASS/PASS/WARN/PASS"',
  overrepresented_sequences_assessement             varchar(255)         DEFAULT NULL                                           COMMENT 'FastQC "PASS", "WARN", "FAIL" per input file. Array of strings separated by "/", with a "," separating entries for paired-end 1 and 2 reads. e.g. Four RG "PASS/PASS/WARN/PASS,PASS/PASS/WARN/PASS"',
  n_content_per_base_assessement                    varchar(255)         DEFAULT NULL                                           COMMENT 'FastQC "PASS", "WARN", "FAIL" per input file. Array of strings separated by "/", with a "," separating entries for paired-end 1 and 2 reads. e.g. Four RG "PASS/PASS/WARN/PASS,PASS/PASS/WARN/PASS"',
  sequence_content_per_base_assessement             varchar(255)         DEFAULT NULL                                           COMMENT 'FastQC "PASS", "WARN", "FAIL" per input file. Array of strings separated by "/", with a "," separating entries for paired-end 1 and 2 reads. e.g. Four RG "PASS/PASS/WARN/PASS,PASS/PASS/WARN/PASS"',
  sequence_quality_per_base_assessement             varchar(255)         DEFAULT NULL                                           COMMENT 'FastQC "PASS", "WARN", "FAIL" per input file. Array of strings separated by "/", with a "," separating entries for paired-end 1 and 2 reads. e.g. Four RG "PASS/PASS/WARN/PASS,PASS/PASS/WARN/PASS"',
  gc_content_per_sequence_assessement               varchar(255)         DEFAULT NULL                                           COMMENT 'FastQC "PASS", "WARN", "FAIL" per input file. Array of strings separated by "/", with a "," separating entries for paired-end 1 and 2 reads. e.g. Four RG "PASS/PASS/WARN/PASS,PASS/PASS/WARN/PASS"',
  quality_scores_per_sequence_assessement           varchar(255)         DEFAULT NULL                                           COMMENT 'FastQC "PASS", "WARN", "FAIL" per input file. Array of strings separated by "/", with a "," separating entries for paired-end 1 and 2 reads. e.g. Four RG "PASS/PASS/WARN/PASS,PASS/PASS/WARN/PASS"',
  sequence_duplication_levels_assessement           varchar(255)         DEFAULT NULL                                           COMMENT 'FastQC "PASS", "WARN", "FAIL" per input file. Array of strings separated by "/", with a "," separating entries for paired-end 1 and 2 reads. e.g. Four RG "PASS/PASS/WARN/PASS,PASS/PASS/WARN/PASS"',
  sequence_length_distribution_assessement          varchar(255)         DEFAULT NULL                                           COMMENT 'FastQC "PASS", "WARN", "FAIL" per input file. Array of strings separated by "/", with a "," separating entries for paired-end 1 and 2 reads. e.g. Four RG "PASS/PASS/WARN/PASS,PASS/PASS/WARN/PASS"',
  FastQC_overall_assessment                         char(4)              DEFAULT NULL                                           COMMENT 'FastQC "PASS" or "FAIL"',
  nrd                                               float unsigned       DEFAULT NULL                                           COMMENT 'Sample discordance levels at non-reference genotypes as a fraction',
  nrd_assessment                                    char(4)              DEFAULT NULL                                           COMMENT '"PASS" based on nrd_persent < 2% or "FAIL" or "NA" if genotyping data not available for this sample',
  sex_reported                                      char(6)              DEFAULT NULL                                           COMMENT 'Sex as reported by sample supplier',
  sex_computed                                      char(6)              DEFAULT NULL                                           COMMENT 'Genetic sex as identified by sequence data',
  input_files_status                                char(10)             DEFAULT NULL                                           COMMENT 'Status of the input files, either ''USEABLE'' or ''DELETED''',
  intermediate_files_status                         char(10)             DEFAULT NULL                                           COMMENT 'Status of the intermediate files, either ''USEABLE'' or ''DELETED''',
  output_files_status                               char(10)             DEFAULT NULL                                           COMMENT 'Status of the output files, either ''ARCHIVED'', ''USEABLE'' or ''DELETED''',
  input_status_override_ref                         varchar(255)         DEFAULT NULL                                           COMMENT 'Status override reference for the input files',
  intermediate_status_override_ref                  varchar(255)         DEFAULT NULL                                           COMMENT 'Status override reference for the intermediate files',
  output_status_override_ref                        varchar(255)         DEFAULT NULL                                           COMMENT 'Status override reference for the output files',

  PRIMARY KEY (id_iseq_ext_pr_metrics_tmp),
  UNIQUE KEY iseq_ext_pr_file_path (file_path),
  KEY iseq_ext_pr_manifest_status (manifest_upload_status),
  KEY iseq_ext_pr_prstatus (processing_status),
  KEY iseq_ext_pr_qc (qc_overall_assessment),
  KEY iseq_ext_pr_instrument (instrument_id),
  KEY iseq_ext_pr_flowcell (flowcell_id),
  KEY iseq_ext_pr_fname (file_name),
  KEY iseq_ext_pr_lib_id (library_id),
  KEY iseq_ext_pr_sample_name (supplier_sample_name),
  KEY iseq_ext_pr_plate_bc (plate_barcode),
  KEY iseq_ext_pr_id_product (id_iseq_product),
  KEY iseq_ext_pr_id_run (id_run)
) ENGINE=InnoDB AUTO_INCREMENT=212627 DEFAULT CHARSET=latin1 COMMENT='Externally computed metrics for data sequenced at WSI';



CREATE TABLE iseq_flowcell (

  id_iseq_flowcell_tmp        int(10) unsigned      NOT NULL AUTO_INCREMENT  COMMENT 'Internal to this database id, value can change',
  last_updated                datetime              NOT NULL                 COMMENT 'Timestamp of last update',
  recorded_at                 datetime              NOT NULL                 COMMENT 'Timestamp of warehouse update',
  id_sample_tmp               int(10) unsigned      NOT NULL                 COMMENT 'Sample id, see "sample.id_sample_tmp"',
  id_study_tmp                int(10) unsigned      DEFAULT NULL             COMMENT 'Study id, see "study.id_study_tmp"',
  cost_code                   varchar(20)           DEFAULT NULL             COMMENT 'Valid WTSI cost code',
  is_r_and_d                  tinyint(1)            DEFAULT '0'              COMMENT 'A boolean flag derived from cost code, flags RandD',
  id_lims                     varchar(10)           NOT NULL                 COMMENT 'LIM system identifier, e.g. CLARITY-GCLP, SEQSCAPE',
  priority                    smallint(2) unsigned  DEFAULT '1'              COMMENT 'Priority',
  manual_qc                   tinyint(1)            DEFAULT NULL             COMMENT 'Legacy QC decision value set per lane which may be used for per-lane billing: iseq_product_metrics.qc is likely to contain the per product QC summary of use to most downstream users',
  external_release            tinyint(1)            DEFAULT NULL             COMMENT 'Defaults to manual qc value; can be changed by the user later',
  flowcell_barcode            varchar(15)           DEFAULT NULL             COMMENT 'Manufacturer flowcell barcode or other identifier',
  id_flowcell_lims            varchar(20)           NOT NULL                 COMMENT 'LIMs-specific flowcell id, batch_id for Sequencescape',
  position                    smallint(2) unsigned  NOT NULL                 COMMENT 'Flowcell lane number',
  entity_type                 varchar(30)           NOT NULL                 COMMENT 'Lane type: library, pool, library_control, library_indexed, library_indexed_spike',
  entity_id_lims              varchar(20)           NOT NULL                 COMMENT 'Most specific LIMs identifier associated with this lane or plex or spike',
  tag_index                   smallint(5) unsigned  DEFAULT NULL             COMMENT 'Tag index, NULL if lane is not a pool',
  tag_sequence                varchar(30)           DEFAULT NULL             COMMENT 'Tag sequence',
  tag_set_id_lims             varchar(20)           DEFAULT NULL             COMMENT 'LIMs-specific identifier of the tag set',
  tag_set_name                varchar(100)          DEFAULT NULL             COMMENT 'WTSI-wide tag set name',
  tag_identifier              varchar(30)           DEFAULT NULL             COMMENT 'The position of tag within the tag group',
  tag2_sequence               varchar(30)           DEFAULT NULL             COMMENT 'Tag sequence for tag 2',
  tag2_set_id_lims            varchar(20)           DEFAULT NULL             COMMENT 'LIMs-specific identifier of the tag set for tag 2',
  tag2_set_name               varchar(100)          DEFAULT NULL             COMMENT 'WTSI-wide tag set name for tag 2',
  tag2_identifier             varchar(30)           DEFAULT NULL             COMMENT 'The position of tag2 within the tag group',
  is_spiked                   tinyint(1)            NOT NULL DEFAULT '0'     COMMENT 'Boolean flag indicating presence of a spike',
  pipeline_id_lims            varchar(60)           DEFAULT NULL             COMMENT 'LIMs-specific pipeline identifier that unambiguously defines library type',
  bait_name                   varchar(50)           DEFAULT NULL             COMMENT 'WTSI-wide name that uniquely identifies a bait set',
  requested_insert_size_from  int(5) unsigned       DEFAULT NULL             COMMENT 'Requested insert size min value',
  requested_insert_size_to    int(5) unsigned       DEFAULT NULL             COMMENT 'Requested insert size max value',
  forward_read_length         smallint(4) unsigned  DEFAULT NULL             COMMENT 'Requested forward read length, bp',
  reverse_read_length         smallint(4) unsigned  DEFAULT NULL             COMMENT 'Requested reverse read length, bp',
  id_pool_lims                varchar(20)           NOT NULL                 COMMENT 'Most specific LIMs identifier associated with the pool',
  legacy_library_id           int(11)               DEFAULT NULL             COMMENT 'Legacy library_id for backwards compatibility.',
  id_library_lims             varchar(255)          DEFAULT NULL             COMMENT 'Earliest LIMs identifier associated with library creation',
  team                        varchar(255)          DEFAULT NULL             COMMENT 'The team responsible for creating the flowcell',
  purpose                     varchar(30)           DEFAULT NULL             COMMENT 'Describes the reason the sequencing was conducted. Eg. Standard, QC, Control',
  suboptimal                  tinyint(1)            DEFAULT NULL             COMMENT 'Indicates that a sample has failed a QC step during processing',
  primer_panel                varchar(255)          DEFAULT NULL             COMMENT 'Primer Panel name',
  spiked_phix_barcode         varchar(20)           DEFAULT NULL             COMMENT 'Barcode of the PhiX tube added to the lane',
  spiked_phix_percentage      float                 DEFAULT NULL             COMMENT 'Percentage PhiX tube spiked in the pool in terms of molar concentration',
  loading_concentration       float                 DEFAULT NULL             COMMENT 'Final instrument loading concentration (pM)',
  workflow                    varchar(20)           DEFAULT NULL             COMMENT 'Workflow used when processing the flowcell',

  PRIMARY KEY (id_iseq_flowcell_tmp),
  UNIQUE KEY index_iseq_flowcell_id_flowcell_lims_position_tag_index_id_lims (id_flowcell_lims,position,tag_index,id_lims),
  KEY iseq_flowcell_id_lims_id_flowcell_lims_index (id_lims,id_flowcell_lims),
  KEY iseq_flowcell_sample_fk (id_sample_tmp),
  KEY iseq_flowcell_study_fk (id_study_tmp),
  KEY index_iseq_flowcell_on_id_pool_lims (id_pool_lims),
  KEY index_iseq_flowcell_on_id_library_lims (id_library_lims),
  KEY index_iseqflowcell__id_flowcell_lims__position__tag_index (id_flowcell_lims,position,tag_index),
  KEY index_iseqflowcell__flowcell_barcode__position__tag_index (flowcell_barcode,position,tag_index),
  KEY index_iseq_flowcell_legacy_library_id (legacy_library_id),
  CONSTRAINT iseq_flowcell_sample_fk FOREIGN KEY (id_sample_tmp) REFERENCES sample (id_sample_tmp) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT iseq_flowcell_study_fk FOREIGN KEY (id_study_tmp) REFERENCES study (id_study_tmp) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=12148736 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



CREATE TABLE iseq_heron_climb_status (

  id_iseq_product                  char(64)          DEFAULT NULL,
  supplier_sample_name             varchar(255)      DEFAULT NULL,
  climb_upload                     datetime          DEFAULT NULL,
  folder_name                      varchar(64)       DEFAULT NULL,
  climb_biosample_metadata_upload  datetime          DEFAULT NULL,
  cog_sample_meta                  tinyint(1)        DEFAULT NULL,
  climb_sequence_metadata_upload   datetime          DEFAULT NULL,
  id                               int(10) unsigned  NOT NULL AUTO_INCREMENT,
  anonymous_sample_id              varchar(15)       DEFAULT '',

  PRIMARY KEY (id),
  KEY ihcs_supplier_sample_name (supplier_sample_name),
  KEY id_iseq_product_idx (id_iseq_product),
  KEY ihcs_climb_upload_idx (climb_upload),
  KEY ihcs_folder_name_idx (folder_name),
  KEY anonymous_sample_id_idx (anonymous_sample_id)
) ENGINE=InnoDB AUTO_INCREMENT=5274840 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



CREATE TABLE iseq_heron_product_metrics (

  id_iseq_hrpr_metrics_tmp  bigint(20) unsigned   NOT NULL AUTO_INCREMENT                                COMMENT 'Internal to this database id, value can change',
  created                   datetime              DEFAULT CURRENT_TIMESTAMP                              COMMENT 'Datetime this record was created',
  last_changed              datetime              DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP  COMMENT 'Datetime this record was created or changed',
  id_run                    int(10) unsigned      DEFAULT NULL                                           COMMENT 'Run id',
  id_iseq_product           char(64)              NOT NULL                                               COMMENT 'Product id, a foreign key into iseq_product_metrics table',
  supplier_sample_name      varchar(255)          DEFAULT NULL                                           COMMENT 'Sample name given by the supplier, as recorded by WSI',
  pp_name                   varchar(40)           DEFAULT 'ncov2019-artic-nf'                            COMMENT 'The name of the pipeline that produced the QC metric',
  pp_version                varchar(40)           DEFAULT NULL                                           COMMENT 'The version of the pipeline specified in the pp_name column',
  pp_repo_url               varchar(255)          DEFAULT NULL                                           COMMENT 'URL of the VCS repository for this pipeline',
  artic_qc_outcome          char(15)              DEFAULT NULL                                           COMMENT 'Artic pipeline QC outcome, "TRUE", "FALSE" or a NULL value',
  climb_upload              datetime              DEFAULT NULL                                           COMMENT 'Datetime files for this sample were uploaded to CLIMB',
  cog_sample_meta           tinyint(1) unsigned   DEFAULT NULL                                           COMMENT 'A Boolean flag to mark sample metadata upload to COG',
  path_root                 varchar(255)          DEFAULT NULL                                           COMMENT 'The uploaded files path root for the entity',
  ivar_md                   smallint(5) unsigned  DEFAULT NULL                                           COMMENT 'ivar minimum depth used in generating the default consensus',
  pct_N_bases               float                 DEFAULT NULL                                           COMMENT 'Percent of N bases',
  pct_covered_bases         float                 DEFAULT NULL                                           COMMENT 'Percent of covered bases',
  longest_no_N_run          smallint(5) unsigned  DEFAULT NULL                                           COMMENT 'Longest consensus data stretch without N',
  ivar_amd                  smallint(5) unsigned  DEFAULT NULL                                           COMMENT 'ivar minimum depth used in generating the additional consensus',
  pct_N_bases_amd           float                 DEFAULT NULL                                           COMMENT 'Percent of N bases in the additional consensus',
  longest_no_N_run_amd      smallint(5) unsigned  DEFAULT NULL                                           COMMENT 'Longest data stretch without N in the additional consensus',
  num_aligned_reads         bigint(20) unsigned   DEFAULT NULL                                           COMMENT 'Number of aligned filtered reads',

  PRIMARY KEY (id_iseq_hrpr_metrics_tmp),
  UNIQUE KEY iseq_hrm_digest_unq (id_iseq_product),
  KEY iseq_hrm_ssn (supplier_sample_name),
  KEY iseq_hrm_idrun (id_run),
  KEY iseq_hrm_ppver (pp_version)
) ENGINE=InnoDB AUTO_INCREMENT=3066370 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='Heron project additional metrics';

SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;

 1 AS id_iseq_hrpr_metrics_tmp,
 1 AS created,
 1 AS last_changed,
 1 AS id_run,
 1 AS id_iseq_product,
 1 AS supplier_sample_name,
 1 AS pp_name,
 1 AS pp_version,
 1 AS pp_repo_url,
 1 AS artic_qc_outcome,
 1 AS climb_upload,
 1 AS cog_sample_meta,
 1 AS path_root,
 1 AS ivar_md,
 1 AS pct_N_bases,
 1 AS pct_covered_bases,
 1 AS longest_no_N_run,
 1 AS ivar_amd,
 1 AS pct_N_bases_amd,
 1 AS longest_no_N_run_amd,
 1 AS num_aligned_reads*/;
SET character_set_client = @saved_cs_client;


CREATE TABLE iseq_product_ampliconstats (

  id_iseq_pr_astats_tmp       bigint(20) unsigned   NOT NULL AUTO_INCREMENT                                COMMENT 'Internal to this database id, value can change',
  created                     datetime              DEFAULT CURRENT_TIMESTAMP                              COMMENT 'Datetime this record was created',
  last_changed                datetime              DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP  COMMENT 'Datetime this record was created or changed',
  id_iseq_product             char(64)              NOT NULL                                               COMMENT 'Product id, a foreign key into iseq_product_metrics table',
  primer_panel                varchar(255)          NOT NULL                                               COMMENT 'A string uniquely identifying the primer panel',
  primer_panel_num_amplicons  smallint(5) unsigned  NOT NULL                                               COMMENT 'Total number of amplicons in the primer panel',
  amplicon_index              smallint(5) unsigned  NOT NULL                                               COMMENT 'Amplicon index (position) in the primer panel, from 1 to the value of primer_panel_num_amplicons',
  pp_name                     varchar(40)           NOT NULL                                               COMMENT 'Name of the portable pipeline that generated the data',
  pp_version                  varchar(40)           DEFAULT NULL                                           COMMENT 'Version of the portable pipeline and/or samtools that generated the data',
  metric_FPCOV_1              decimal(5,2)          DEFAULT NULL                                           COMMENT 'Coverage percent at depth 1',
  metric_FPCOV_10             decimal(5,2)          DEFAULT NULL                                           COMMENT 'Coverage percent at depth 10',
  metric_FPCOV_20             decimal(5,2)          DEFAULT NULL                                           COMMENT 'Coverage percent at depth 20',
  metric_FPCOV_100            decimal(5,2)          DEFAULT NULL                                           COMMENT 'Coverage percent at depth 100',
  metric_FREADS               int(10) unsigned      DEFAULT NULL                                           COMMENT 'Number of aligned filtered reads',

  PRIMARY KEY (id_iseq_pr_astats_tmp),
  UNIQUE KEY iseq_hrm_digest_unq (id_iseq_product,primer_panel,amplicon_index),
  KEY iseq_pastats_amplicon (primer_panel_num_amplicons,amplicon_index),
  CONSTRAINT iseq_pastats_prm_fk FOREIGN KEY (id_iseq_product) REFERENCES iseq_product_metrics (id_iseq_product) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=296879389 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='Some of per sample per amplicon metrics generated by samtools ampliconstats';



CREATE TABLE iseq_product_components (

  id_iseq_pr_components_tmp  bigint(20) unsigned  NOT NULL AUTO_INCREMENT  COMMENT 'Internal to this database id, value can change',
  id_iseq_pr_tmp             bigint(20) unsigned  NOT NULL                 COMMENT 'iseq_product_metrics table row id for the product',
  id_iseq_pr_component_tmp   bigint(20) unsigned  NOT NULL                 COMMENT 'iseq_product_metrics table row id for one of this product''s components',
  num_components             tinyint(3) unsigned  NOT NULL                 COMMENT 'Number of component products for this product',
  component_index            tinyint(3) unsigned  NOT NULL                 COMMENT 'Unique component index within all components of this product, \na value from 1 to the value of num_components column for this product',

  PRIMARY KEY (id_iseq_pr_components_tmp),
  UNIQUE KEY iseq_pr_comp_unique (id_iseq_pr_tmp,id_iseq_pr_component_tmp),
  KEY iseq_pr_comp_pr_comp_fk (id_iseq_pr_component_tmp),
  KEY iseq_pr_comp_ncomp (num_components,id_iseq_pr_tmp),
  KEY iseq_pr_comp_compi (component_index,num_components),
  CONSTRAINT iseq_pr_comp_pr_comp_fk FOREIGN KEY (id_iseq_pr_component_tmp) REFERENCES iseq_product_metrics (id_iseq_pr_metrics_tmp) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT iseq_pr_comp_pr_fk FOREIGN KEY (id_iseq_pr_tmp) REFERENCES iseq_product_metrics (id_iseq_pr_metrics_tmp) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=9552617 DEFAULT CHARSET=utf8;



CREATE TABLE iseq_product_metrics (

  id_iseq_pr_metrics_tmp                         bigint(20) unsigned   NOT NULL AUTO_INCREMENT                                COMMENT 'Internal to this database id, value can change',
  id_iseq_product                                char(64)              NOT NULL                                               COMMENT 'Product id',
  last_changed                                   datetime              DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP  COMMENT 'Date this record was created or changed',
  id_iseq_flowcell_tmp                           int(10) unsigned      DEFAULT NULL                                           COMMENT 'Flowcell id, see "iseq_flowcell.id_iseq_flowcell_tmp"',
  id_run                                         int(10) unsigned      DEFAULT NULL                                           COMMENT 'NPG run identifier',
  position                                       smallint(2) unsigned  DEFAULT NULL                                           COMMENT 'Flowcell lane number',
  tag_index                                      smallint(5) unsigned  DEFAULT NULL                                           COMMENT 'Tag index, NULL if lane is not a pool',
  iseq_composition_tmp                           varchar(600)          DEFAULT NULL                                           COMMENT 'JSON representation of the composition object, the column might be deleted in future',
  qc_seq                                         tinyint(1)            DEFAULT NULL                                           COMMENT 'Sequencing lane level QC outcome, a result of either manual or automatic assessment by core',
  qc_lib                                         tinyint(1)            DEFAULT NULL                                           COMMENT 'Library QC outcome, a result of either manual or automatic assessment by core',
  qc_user                                        tinyint(1)            DEFAULT NULL                                           COMMENT 'Library QC outcome according to the data user criteria, a result of either manual or automatic assessment',
  qc                                             tinyint(1)            DEFAULT NULL                                           COMMENT 'Overall QC assessment outcome, a logical product (conjunction) of qc_seq and qc_lib values, defaults to the qc_seq value when qc_lib is not defined',
  tag_sequence4deplexing                         varchar(30)           DEFAULT NULL                                           COMMENT 'Tag sequence used for deplexing the lane, common suffix might have been truncated',
  actual_forward_read_length                     smallint(4) unsigned  DEFAULT NULL                                           COMMENT 'Actual forward read length, bp',
  actual_reverse_read_length                     smallint(4) unsigned  DEFAULT NULL                                           COMMENT 'Actual reverse read length, bp',
  indexing_read_length                           smallint(2) unsigned  DEFAULT NULL                                           COMMENT 'Indexing read length, bp',
  tag_decode_percent                             float(5,2) unsigned   DEFAULT NULL,
  tag_decode_count                               int(10) unsigned      DEFAULT NULL,
  insert_size_quartile1                          smallint(5) unsigned  DEFAULT NULL,
  insert_size_quartile3                          smallint(5) unsigned  DEFAULT NULL,
  insert_size_median                             smallint(5) unsigned  DEFAULT NULL,
  insert_size_num_modes                          smallint(4) unsigned  DEFAULT NULL,
  insert_size_normal_fit_confidence              float(3,2) unsigned   DEFAULT NULL,
  gc_percent_forward_read                        float(5,2) unsigned   DEFAULT NULL,
  gc_percent_reverse_read                        float(5,2) unsigned   DEFAULT NULL,
  sequence_mismatch_percent_forward_read         float(4,2) unsigned   DEFAULT NULL,
  sequence_mismatch_percent_reverse_read         float(4,2) unsigned   DEFAULT NULL,
  adapters_percent_forward_read                  float(5,2) unsigned   DEFAULT NULL,
  adapters_percent_reverse_read                  float(5,2) unsigned   DEFAULT NULL,
  ref_match1_name                                varchar(100)          DEFAULT NULL,
  ref_match1_percent                             float(5,2)            DEFAULT NULL,
  ref_match2_name                                varchar(100)          DEFAULT NULL,
  ref_match2_percent                             float(5,2)            DEFAULT NULL,
  q20_yield_kb_forward_read                      int(10) unsigned      DEFAULT NULL,
  q20_yield_kb_reverse_read                      int(10) unsigned      DEFAULT NULL,
  q30_yield_kb_forward_read                      int(10) unsigned      DEFAULT NULL,
  q30_yield_kb_reverse_read                      int(10) unsigned      DEFAULT NULL,
  q40_yield_kb_forward_read                      int(10) unsigned      DEFAULT NULL,
  q40_yield_kb_reverse_read                      int(10) unsigned      DEFAULT NULL,
  num_reads                                      bigint(20) unsigned   DEFAULT NULL,
  percent_mapped                                 float(5,2)            DEFAULT NULL,
  percent_duplicate                              float(5,2)            DEFAULT NULL,
  chimeric_reads_percent                         float(5,2) unsigned   DEFAULT NULL                                           COMMENT 'mate_mapped_defferent_chr_5 as percentage of all',
  human_percent_mapped                           float(5,2)            DEFAULT NULL,
  human_percent_duplicate                        float(5,2)            DEFAULT NULL,
  genotype_sample_name_match                     varchar(8)            DEFAULT NULL,
  genotype_sample_name_relaxed_match             varchar(8)            DEFAULT NULL,
  genotype_mean_depth                            float(7,2)            DEFAULT NULL,
  mean_bait_coverage                             float(8,2) unsigned   DEFAULT NULL,
  on_bait_percent                                float(5,2) unsigned   DEFAULT NULL,
  on_or_near_bait_percent                        float(5,2) unsigned   DEFAULT NULL,
  verify_bam_id_average_depth                    float(11,2) unsigned  DEFAULT NULL,
  verify_bam_id_score                            float(6,5) unsigned   DEFAULT NULL,
  verify_bam_id_snp_count                        int(10) unsigned      DEFAULT NULL,
  rna_exonic_rate                                float unsigned        DEFAULT NULL                                           COMMENT 'Exonic Rate is the fraction mapping within exons',
  rna_percent_end_2_reads_sense                  float unsigned        DEFAULT NULL                                           COMMENT 'Percentage of intragenic End 2 reads that were sequenced in the sense direction.',
  rna_rrna_rate                                  float unsigned        DEFAULT NULL                                           COMMENT 'rRNA Rate is per total reads',
  rna_genes_detected                             int(10) unsigned      DEFAULT NULL                                           COMMENT 'Number of genes detected with at least 5 reads.',
  rna_norm_3_prime_coverage                      float unsigned        DEFAULT NULL                                           COMMENT '3 prime n-based normalization: n is the transcript length at that end; norm is the ratio between the coverage at the 3 prime end and the average coverage of the full transcript, averaged over all transcripts',
  rna_norm_5_prime_coverage                      float unsigned        DEFAULT NULL                                           COMMENT '5 prime n-based normalization: n is the transcript length at that end; norm is the ratio between the coverage at the 5 prime end and the average coverage of the full transcript, averaged over all transcripts',
  rna_intronic_rate                              float unsigned        DEFAULT NULL                                           COMMENT 'Intronic rate is the fraction mapping within introns',
  rna_transcripts_detected                       int(10) unsigned      DEFAULT NULL                                           COMMENT 'Number of transcripts detected with at least 5 reads',
  rna_globin_percent_tpm                         float unsigned        DEFAULT NULL                                           COMMENT 'Percentage of globin genes TPM (transcripts per million) detected',
  rna_mitochondrial_percent_tpm                  float unsigned        DEFAULT NULL                                           COMMENT 'Percentage of mitochondrial genes TPM (transcripts per million) detected',
  gbs_call_rate                                  float unsigned        DEFAULT NULL                                           COMMENT 'The GbS call rate is the fraction of loci called on the relevant primer panel',
  gbs_pass_rate                                  float unsigned        DEFAULT NULL                                           COMMENT 'The GbS pass rate is the fraction of loci called and passing filters on the relevant primer panel',
  nrd_percent                                    float(5,2)            DEFAULT NULL                                           COMMENT 'Percent of non-reference discordance',
  target_filter                                  varchar(30)           DEFAULT NULL                                           COMMENT 'Filter used to produce the target stats file',
  target_length                                  bigint(12) unsigned   DEFAULT NULL                                           COMMENT 'The total length of the target regions',
  target_mapped_reads                            bigint(20) unsigned   DEFAULT NULL                                           COMMENT 'The number of mapped reads passing the target filter',
  target_proper_pair_mapped_reads                bigint(20) unsigned   DEFAULT NULL                                           COMMENT 'The number of proper pair mapped reads passing the target filter',
  target_mapped_bases                            bigint(20) unsigned   DEFAULT NULL                                           COMMENT 'The number of mapped bases passing the target filter',
  target_coverage_threshold                      int(4)                DEFAULT NULL                                           COMMENT 'The coverage threshold used in the target perc target greater than depth calculation',
  target_percent_gt_coverage_threshold           float(5,2)            DEFAULT NULL                                           COMMENT 'The percentage of the target covered at greater than the depth specified',
  target_autosome_coverage_threshold             int(4)                DEFAULT NULL                                           COMMENT 'The coverage threshold used in the perc target autosome greater than depth calculation',
  target_autosome_percent_gt_coverage_threshold  float(5,2)            DEFAULT NULL                                           COMMENT 'The percentage of the target autosome covered at greater than the depth specified',
  sub_titv_class                                 float unsigned        DEFAULT NULL                                           COMMENT 'The ratio of transition substitution counts to transvertion',
  sub_titv_mean_ca                               float unsigned        DEFAULT NULL                                           COMMENT 'TiTv where count of CA+GT is taken as if it were mean across other transversions',
  sub_frac_sub_hq                                float unsigned        DEFAULT NULL                                           COMMENT 'Fraction of substitutions which are high quality (>=Q30)',
  sub_oxog_bias                                  float unsigned        DEFAULT NULL                                           COMMENT 'How similar CA to GT counts are within each read (high quality >=Q30 substitutions only) in order to detect OxoG oxidative artifacts',
  sub_sym_gt_ca                                  float unsigned        DEFAULT NULL                                           COMMENT 'How symmetrical CA and GT counts are within each read',
  sub_sym_ct_ga                                  float unsigned        DEFAULT NULL                                           COMMENT 'How symmetrical CT and GA counts are within each read',
  sub_sym_ag_tc                                  float unsigned        DEFAULT NULL                                           COMMENT 'How symmetrical AG and TC counts are within each read',
  sub_cv_ti                                      float unsigned        DEFAULT NULL                                           COMMENT 'Coefficient of variation across all Ti substitutions = std(Ti)/mean(Ti)',
  sub_gt_ti                                      float unsigned        DEFAULT NULL                                           COMMENT 'Computed as a maximum between (i) ratio of GT counts to TC and (ii) ratio CA to GA',
  sub_gt_mean_ti                                 float unsigned        DEFAULT NULL                                           COMMENT 'Computed as a maximum between (i) ratio of GT counts to mean(Ti) and (ii) ratio CA to mean(Ti)',
  sub_ctoa_oxh                                   float unsigned        DEFAULT NULL                                           COMMENT 'This metric is used to compute the likelihood of C2A and its predicted level',
  sub_ctoa_art_predicted_level                   tinyint(1) unsigned   DEFAULT NULL                                           COMMENT 'C2A predicted level - 0 = not present, 1 = low, 2 = medium and 3 = high',

  PRIMARY KEY (id_iseq_pr_metrics_tmp),
  UNIQUE KEY iseq_pr_metrics_product_unique (id_iseq_product),
  KEY iseq_pm_fcid_run_pos_tag_index (id_run,position,tag_index),
  KEY iseq_pr_metrics_flc_fk (id_iseq_flowcell_tmp),
  CONSTRAINT iseq_pr_metrics_flc_fk FOREIGN KEY (id_iseq_flowcell_tmp) REFERENCES iseq_flowcell (id_iseq_flowcell_tmp) ON DELETE SET NULL ON UPDATE NO ACTION,
  CONSTRAINT iseq_pr_metrics_lm_fk FOREIGN KEY (id_run, position) REFERENCES iseq_run_lane_metrics (id_run, position) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=2109050904 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



CREATE TABLE iseq_run (

  id_run                            int(10) unsigned      NOT NULL      COMMENT 'NPG run identifier',
  id_flowcell_lims                  varchar(20)           DEFAULT NULL  COMMENT 'LIMS specific flowcell id',
  folder_name                       varchar(64)           DEFAULT NULL  COMMENT 'Runfolder name',
  rp__read1_number_of_cycles        smallint(5) unsigned  DEFAULT NULL  COMMENT 'Read 1 number of cycles',
  rp__read2_number_of_cycles        smallint(5) unsigned  DEFAULT NULL  COMMENT 'Read 2 number of cycles',
  rp__flow_cell_mode                varchar(4)            DEFAULT NULL  COMMENT 'Flowcell mode',
  rp__workflow_type                 varchar(16)           DEFAULT NULL  COMMENT 'Workflow type',
  rp__flow_cell_consumable_version  varchar(4)            DEFAULT NULL  COMMENT 'Flowcell consumable version',
  rp__sbs_consumable_version        varchar(4)            DEFAULT NULL  COMMENT 'Sbs consumable version',

  PRIMARY KEY (id_run),
  KEY iseq_run_id_flowcell_lims (id_flowcell_lims)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='Table linking run and flowcell identities with the run folder name';



CREATE TABLE iseq_run_info (

  id_run              int(10) unsigned  NOT NULL  COMMENT 'NPG run identifier',
  run_parameters_xml  text                        COMMENT 'The contents of Illumina''s {R,r}unParameters.xml file',

  PRIMARY KEY (id_run),
  CONSTRAINT iseq_run_info_ibfk_1 FOREIGN KEY (id_run) REFERENCES iseq_run (id_run) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='Table storing selected text files from the run folder';



CREATE TABLE iseq_run_lane_metrics (

  flowcell_barcode                  varchar(15)            DEFAULT NULL                                           COMMENT 'Manufacturer flowcell barcode or other identifier as recorded by NPG',
  id_run                            int(10) unsigned       NOT NULL                                               COMMENT 'NPG run identifier',
  position                          smallint(2) unsigned   NOT NULL                                               COMMENT 'Flowcell lane number',
  last_changed                      datetime               DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP  COMMENT 'Date this record was created or changed',
  qc_seq                            tinyint(1)             DEFAULT NULL                                           COMMENT 'Sequencing lane level QC outcome, a result of either manual or automatic assessment by core',
  instrument_name                   char(32)               DEFAULT NULL,
  instrument_external_name          char(10)               DEFAULT NULL                                           COMMENT 'Name assigned to the instrument by the manufacturer',
  instrument_model                  char(64)               DEFAULT NULL,
  instrument_side                   char(1)                DEFAULT NULL                                           COMMENT 'Illumina instrument side (A or B), if appropriate',
  workflow_type                     varchar(20)            DEFAULT NULL                                           COMMENT 'Illumina instrument workflow type',
  paired_read                       tinyint(1) unsigned    NOT NULL DEFAULT '0',
  cycles                            int(4) unsigned        NOT NULL,
  cancelled                         tinyint(1) unsigned    NOT NULL DEFAULT '0'                                   COMMENT 'Boolen flag to indicate whether the run was cancelled',
  run_pending                       datetime               DEFAULT NULL                                           COMMENT 'Timestamp of run pending status',
  run_complete                      datetime               DEFAULT NULL                                           COMMENT 'Timestamp of run complete status',
  qc_complete                       datetime               DEFAULT NULL                                           COMMENT 'Timestamp of qc complete status',
  pf_cluster_count                  bigint(20) unsigned    DEFAULT NULL,
  raw_cluster_count                 bigint(20) unsigned    DEFAULT NULL,
  raw_cluster_density               double(12,3) unsigned  DEFAULT NULL,
  pf_cluster_density                double(12,3) unsigned  DEFAULT NULL,
  pf_bases                          bigint(20) unsigned    DEFAULT NULL,
  q20_yield_kb_forward_read         int(10) unsigned       DEFAULT NULL,
  q20_yield_kb_reverse_read         int(10) unsigned       DEFAULT NULL,
  q30_yield_kb_forward_read         int(10) unsigned       DEFAULT NULL,
  q30_yield_kb_reverse_read         int(10) unsigned       DEFAULT NULL,
  q40_yield_kb_forward_read         int(10) unsigned       DEFAULT NULL,
  q40_yield_kb_reverse_read         int(10) unsigned       DEFAULT NULL,
  tags_decode_percent               float(5,2) unsigned    DEFAULT NULL,
  tags_decode_cv                    float(6,2) unsigned    DEFAULT NULL,
  unexpected_tags_percent           float(5,2) unsigned    DEFAULT NULL                                           COMMENT 'tag0_perfect_match_reads as a percentage of total_lane_reads',
  tag_hops_percent                  float unsigned         DEFAULT NULL                                           COMMENT 'Percentage tag hops for dual index runs',
  tag_hops_power                    float unsigned         DEFAULT NULL                                           COMMENT 'Power to detect tag hops for dual index runs',
  run_priority                      tinyint(3)             DEFAULT NULL                                           COMMENT 'Sequencing lane level run priority, a result of either manual or default value set by core',
  interop_cluster_count_total       bigint(20) unsigned    DEFAULT NULL                                           COMMENT 'Total cluster count for this lane (derived from Illumina InterOp files)',
  interop_cluster_count_mean        double unsigned        DEFAULT NULL                                           COMMENT 'Total cluster count, mean value over tiles of this lane (derived from Illumina InterOp files)',
  interop_cluster_count_stdev       double unsigned        DEFAULT NULL                                           COMMENT 'Standard deviation value for interop_cluster_count_mean',
  interop_cluster_count_pf_total    bigint(20) unsigned    DEFAULT NULL                                           COMMENT 'Purity-filtered cluster count for this lane (derived from Illumina InterOp files)',
  interop_cluster_count_pf_mean     double unsigned        DEFAULT NULL                                           COMMENT 'Purity-filtered cluster count, mean value over tiles of this lane (derived from Illumina InterOp files)',
  interop_cluster_count_pf_stdev    double unsigned        DEFAULT NULL                                           COMMENT 'Standard deviation value for interop_cluster_count_pf_mean',
  interop_cluster_density_mean      double unsigned        DEFAULT NULL                                           COMMENT 'Cluster density, mean value over tiles of this lane (derived from Illumina InterOp files)',
  interop_cluster_density_stdev     double unsigned        DEFAULT NULL                                           COMMENT 'Standard deviation value for interop_cluster_density_mean',
  interop_cluster_density_pf_mean   double unsigned        DEFAULT NULL                                           COMMENT 'Purity-filtered cluster density, mean value over tiles of this lane (derived from Illumina InterOp files)',
  interop_cluster_density_pf_stdev  double unsigned        DEFAULT NULL                                           COMMENT 'Standard deviation value for interop_cluster_density_pf_mean',
  interop_cluster_pf_mean           float(5,2) unsigned    DEFAULT NULL                                           COMMENT ' Percent of purity-filtered clusters, mean value over tiles of this lane (derived from Illumina InterOp files)',
  interop_cluster_pf_stdev          float(5,2) unsigned    DEFAULT NULL                                           COMMENT 'Standard deviation value for interop_cluster_pf_mean',
  interop_occupied_mean             float(5,2) unsigned    DEFAULT NULL                                           COMMENT 'Percent of occupied flowcell wells, a mean value over tiles of this lane (derived from Illumina InterOp files)',
  interop_occupied_stdev            float(5,2) unsigned    DEFAULT NULL                                           COMMENT 'Standard deviation value for interop_occupied_mean',

  PRIMARY KEY (id_run,position),
  KEY iseq_rlmm_id_run_index (id_run),
  KEY iseq_rlm_cancelled_and_run_pending_index (cancelled,run_pending),
  KEY iseq_rlm_cancelled_and_run_complete_index (cancelled,run_complete)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



CREATE TABLE iseq_run_status (

  id_run_status       int(11) unsigned  NOT NULL,
  id_run              int(10) unsigned  NOT NULL   COMMENT 'NPG run identifier',
  `date`              datetime          NOT NULL   COMMENT 'Status timestamp',
  id_run_status_dict  int(10) unsigned  NOT NULL   COMMENT 'Status identifier, see iseq_run_status_dict.id_run_status_dict',
  iscurrent           tinyint(1)        NOT NULL   COMMENT 'Boolean flag, 1 is the status is current, 0 otherwise',

  PRIMARY KEY (id_run_status),
  KEY iseq_run_status_rsd_fk (id_run_status_dict),
  KEY id_run_status_id_run (id_run),
  CONSTRAINT iseq_run_status_rsd_fk FOREIGN KEY (id_run_status_dict) REFERENCES iseq_run_status_dict (id_run_status_dict) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



CREATE TABLE iseq_run_status_dict (

  id_run_status_dict  int(10) unsigned      NOT NULL,
  description         varchar(64)           NOT NULL,
  iscurrent           tinyint(3) unsigned   NOT NULL,
  temporal_index      smallint(5) unsigned  DEFAULT NULL,

  PRIMARY KEY (id_run_status_dict),
  KEY iseq_run_status_dict_description_index (description)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



CREATE TABLE lighthouse_sample (

  id                           int(11)        NOT NULL AUTO_INCREMENT,
  mongodb_id                   varchar(255)   DEFAULT NULL                                                        COMMENT 'Auto-generated id from MongoDB',
  root_sample_id               varchar(255)   NOT NULL                                                            COMMENT 'Id for this sample provided by the Lighthouse lab',
  cog_uk_id                    varchar(255)   NOT NULL,
  cog_uk_id_unique             tinyint(1)     DEFAULT '1'                                                         COMMENT 'A flag to indicate cog_uk_id should be unique. NULL allows reuse of the ID in another row.',
  rna_id                       varchar(255)   NOT NULL                                                            COMMENT 'Lighthouse lab-provided id made up of plate barcode and well',
  plate_barcode                varchar(255)   DEFAULT NULL                                                        COMMENT 'Barcode of plate sample arrived in, from rna_id',
  coordinate                   varchar(255)   DEFAULT NULL                                                        COMMENT 'Well position from plate sample arrived in, from rna_id',
  result                       varchar(255)   NOT NULL                                                            COMMENT 'Covid-19 test result from the Lighthouse lab',
  date_tested_string           varchar(255)   DEFAULT NULL                                                        COMMENT 'When the covid-19 test was carried out by the Lighthouse lab',
  date_tested                  datetime       DEFAULT NULL                                                        COMMENT 'date_tested_string in date format',
  `source`                     varchar(255)   DEFAULT NULL                                                        COMMENT 'Lighthouse centre that the sample came from',
  lab_id                       varchar(255)   DEFAULT NULL                                                        COMMENT 'Id of the lab, within the Lighthouse centre',
  ch1_target                   varchar(255)   DEFAULT NULL,
  ch1_result                   varchar(255)   DEFAULT NULL,
  ch1_cq                       decimal(11,8)  DEFAULT NULL,
  ch2_target                   varchar(255)   DEFAULT NULL,
  ch2_result                   varchar(255)   DEFAULT NULL,
  ch2_cq                       decimal(11,8)  DEFAULT NULL,
  ch3_target                   varchar(255)   DEFAULT NULL,
  ch3_result                   varchar(255)   DEFAULT NULL,
  ch3_cq                       decimal(11,8)  DEFAULT NULL,
  ch4_target                   varchar(255)   DEFAULT NULL,
  ch4_result                   varchar(255)   DEFAULT NULL,
  ch4_cq                       decimal(11,8)  DEFAULT NULL,
  filtered_positive            tinyint(1)     DEFAULT NULL                                                        COMMENT 'Filtered positive result value',
  filtered_positive_version    varchar(255)   DEFAULT NULL                                                        COMMENT 'Filtered positive version',
  filtered_positive_timestamp  datetime       DEFAULT NULL                                                        COMMENT 'Filtered positive timestamp',
  lh_sample_uuid               varchar(36)    DEFAULT NULL                                                        COMMENT 'Sample uuid created in crawler',
  lh_source_plate_uuid         varchar(36)    DEFAULT NULL                                                        COMMENT 'Source plate uuid created in crawler',
  created_at                   datetime       DEFAULT NULL                                                        COMMENT 'When this record was inserted',
  updated_at                   datetime       DEFAULT NULL                                                        COMMENT 'When this record was last updated',
  must_sequence                tinyint(1)     DEFAULT NULL                                                        COMMENT 'PAM provided value whether sample is of high importance',
  preferentially_sequence      tinyint(1)     DEFAULT NULL                                                        COMMENT 'PAM provided value whether sample is important',
  is_current                   tinyint(1)     NOT NULL DEFAULT '0'                                                COMMENT 'Identifies if this sample has the most up to date information for the same rna_id',
  current_rna_id               varchar(255)   GENERATED ALWAYS AS (if((`is_current` = 1),`rna_id`,NULL)) STORED,

  PRIMARY KEY (id),
  UNIQUE KEY index_lighthouse_sample_on_root_sample_id_and_rna_id_and_result (root_sample_id,rna_id,result),
  UNIQUE KEY index_lighthouse_sample_on_mongodb_id (mongodb_id),
  UNIQUE KEY index_lighthouse_sample_on_lh_sample_uuid (lh_sample_uuid),
  UNIQUE KEY index_lighthouse_sample_on_current_rna_id (current_rna_id),
  UNIQUE KEY index_lighthouse_sample_on_cog_uk_id_and_cog_uk_id_unique (cog_uk_id,cog_uk_id_unique),
  KEY index_lighthouse_sample_on_date_tested (date_tested),
  KEY index_lighthouse_sample_on_filtered_positive (filtered_positive),
  KEY index_lighthouse_sample_on_cog_uk_id (cog_uk_id),
  KEY index_lighthouse_sample_on_rna_id (rna_id),
  KEY index_lighthouse_sample_on_plate_barcode_and_created_at (plate_barcode,created_at),
  KEY index_lighthouse_sample_on_result (result)
) ENGINE=InnoDB AUTO_INCREMENT=58489448 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



CREATE TABLE long_read_qc_result (

  id_long_read_qc_result_tmp   bigint(20)    NOT NULL AUTO_INCREMENT,
  labware_barcode              varchar(255)  NOT NULL                  COMMENT 'Barcode of the labware that was the source for the QC tests.',
  sample_id                    varchar(255)  NOT NULL                  COMMENT 'External identifier for the sample(s).',
  assay_type                   varchar(255)  NOT NULL                  COMMENT 'Type of the QC test.',
  assay_type_key               varchar(255)  NOT NULL                  COMMENT 'Unique identifier of the QC test.',
  units                        varchar(255)  DEFAULT NULL              COMMENT 'Unit of the value for example mg,ng etc',
  `value`                      varchar(255)  NOT NULL                  COMMENT 'QC result value',
  id_lims                      varchar(255)  DEFAULT NULL              COMMENT 'Identifier of the LIMS where QC was published from',
  id_long_read_qc_result_lims  varchar(255)  DEFAULT NULL              COMMENT 'LIMS specific id for QC result',
  created                      datetime      DEFAULT NULL              COMMENT 'The date the qc_result was first created in LIMS',
  last_updated                 datetime      DEFAULT NULL              COMMENT 'The date the qc_result was last updated in LIMS.',
  recorded_at                  datetime      DEFAULT NULL              COMMENT 'Timestamp of the latest warehouse update.',
  qc_status                    varchar(255)  DEFAULT NULL              COMMENT 'Status of the QC decision eg pass, fail etc',
  qc_status_decision_by        varchar(255)  DEFAULT NULL              COMMENT 'Who made the QC status decision eg ToL, Long Read',

  PRIMARY KEY (id_long_read_qc_result_tmp)
) ENGINE=InnoDB AUTO_INCREMENT=16679 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



CREATE TABLE oseq_flowcell (

  id_oseq_flowcell_tmp  int(10) unsigned  NOT NULL AUTO_INCREMENT,
  id_flowcell_lims      varchar(255)      NOT NULL                  COMMENT 'LIMs-specific flowcell id',
  last_updated          datetime          NOT NULL                  COMMENT 'Timestamp of last update',
  recorded_at           datetime          NOT NULL                  COMMENT 'Timestamp of warehouse update',
  id_sample_tmp         int(10) unsigned  NOT NULL                  COMMENT 'Sample id, see "sample.id_sample_tmp"',
  id_study_tmp          int(10) unsigned  NOT NULL                  COMMENT 'Study id, see "study.id_study_tmp"',
  experiment_name       varchar(255)      NOT NULL                  COMMENT 'The name of the experiment, eg. The lims generated run id',
  instrument_name       varchar(255)      NOT NULL                  COMMENT 'The name of the instrument on which the sample was run',
  instrument_slot       int(11)           NOT NULL                  COMMENT 'The numeric identifier of the slot on which the sample was run',
  pipeline_id_lims      varchar(255)      DEFAULT NULL              COMMENT 'LIMs-specific pipeline identifier that unambiguously defines library type',
  requested_data_type   varchar(255)      DEFAULT NULL              COMMENT 'The type of data produced by sequencing, eg. basecalls only',
  deleted_at            datetime          DEFAULT NULL              COMMENT 'Timestamp of any flowcell destruction',
  id_lims               varchar(10)       NOT NULL                  COMMENT 'LIM system identifier',
  tag_identifier        varchar(255)      DEFAULT NULL              COMMENT 'Position of the first tag within the tag group',
  tag_sequence          varchar(255)      DEFAULT NULL              COMMENT 'Sequence of the first tag',
  tag_set_id_lims       varchar(255)      DEFAULT NULL              COMMENT 'LIMs-specific identifier of the tag set for the first tag',
  tag_set_name          varchar(255)      DEFAULT NULL              COMMENT 'WTSI-wide tag set name for the first tag',
  tag2_identifier       varchar(255)      DEFAULT NULL              COMMENT 'Position of the second tag within the tag group',
  tag2_sequence         varchar(255)      DEFAULT NULL              COMMENT 'Sequence of the second tag',
  tag2_set_id_lims      varchar(255)      DEFAULT NULL              COMMENT 'LIMs-specific identifier of the tag set for the second tag',
  tag2_set_name         varchar(255)      DEFAULT NULL              COMMENT 'WTSI-wide tag set name for the second tag',
  flowcell_id           varchar(255)      DEFAULT NULL              COMMENT 'The id of the flowcell. Supplied with the flowcell. Format FAVnnnn',
  library_tube_uuid     varchar(36)       DEFAULT NULL              COMMENT 'The uuid for the originating library tube',
  library_tube_barcode  varchar(255)      DEFAULT NULL              COMMENT 'The barcode for the originating library tube',
  run_uuid              varchar(36)       DEFAULT NULL              COMMENT 'The uuid of the run',
  run_id                varchar(255)      DEFAULT NULL              COMMENT 'Run identifier assigned by MinKNOW',

  PRIMARY KEY (id_oseq_flowcell_tmp),
  KEY fk_oseq_flowcell_to_sample (id_sample_tmp),
  KEY fk_oseq_flowcell_to_study (id_study_tmp),
  CONSTRAINT fk_oseq_flowcell_to_sample FOREIGN KEY (id_sample_tmp) REFERENCES sample (id_sample_tmp),
  CONSTRAINT fk_oseq_flowcell_to_study FOREIGN KEY (id_study_tmp) REFERENCES study (id_study_tmp)
) ENGINE=InnoDB AUTO_INCREMENT=4053 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



CREATE TABLE pac_bio_product_metrics (

  id_pac_bio_pr_metrics_tmp  int(11)     NOT NULL AUTO_INCREMENT,
  id_pac_bio_rw_metrics_tmp  int(11)     NOT NULL                     COMMENT 'PacBio run well metrics id, see "pac_bio_run_well_metrics.id_pac_bio_rw_metrics_tmp"',
  id_pac_bio_tmp             int(11)     DEFAULT NULL                 COMMENT 'PacBio run id, see "pac_bio_run.id_pac_bio_tmp"',
  id_pac_bio_product         char(64)    CHARACTER SET utf8 NOT NULL  COMMENT 'Product id',
  qc                         tinyint(1)  DEFAULT NULL                 COMMENT 'The final QC outcome of the product as 0(failed), 1(passed) or NULL',

  PRIMARY KEY (id_pac_bio_pr_metrics_tmp),
  UNIQUE KEY pac_bio_pr_metrics_id_product (id_pac_bio_product),
  UNIQUE KEY pac_bio_metrics_product (id_pac_bio_tmp,id_pac_bio_rw_metrics_tmp),
  KEY pac_bio_pr_metrics_to_rwm_fk (id_pac_bio_rw_metrics_tmp),
  KEY pb_product_qc_index (qc),
  CONSTRAINT pac_bio_product_metrics_to_run_fk FOREIGN KEY (id_pac_bio_tmp) REFERENCES pac_bio_run (id_pac_bio_tmp) ON DELETE SET NULL ON UPDATE NO ACTION,
  CONSTRAINT pac_bio_product_metrics_to_rwm_fk FOREIGN KEY (id_pac_bio_rw_metrics_tmp) REFERENCES pac_bio_run_well_metrics (id_pac_bio_rw_metrics_tmp) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=23798 DEFAULT CHARSET=utf8 COMMENT='A linking table for the pac_bio_run and pac_bio_run_well_metrics tables with a potential for adding per-product QC data';



CREATE TABLE pac_bio_run (

  id_pac_bio_tmp                  int(11)           NOT NULL AUTO_INCREMENT,
  last_updated                    datetime          NOT NULL                                                       COMMENT 'Timestamp of last update',
  recorded_at                     datetime          NOT NULL                                                       COMMENT 'Timestamp of warehouse update',
  id_sample_tmp                   int(10) unsigned  NOT NULL                                                       COMMENT 'Sample id, see "sample.id_sample_tmp"',
  id_study_tmp                    int(10) unsigned  NOT NULL                                                       COMMENT 'Sample id, see "study.id_study_tmp"',
  id_pac_bio_run_lims             varchar(20)       NOT NULL                                                       COMMENT 'Lims specific identifier for the pacbio run',
  pac_bio_run_uuid                varchar(36)       DEFAULT NULL                                                   COMMENT 'Uuid identifier for the pacbio run',
  cost_code                       varchar(20)       NOT NULL                                                       COMMENT 'Valid WTSI cost-code',
  id_lims                         varchar(10)       NOT NULL                                                       COMMENT 'LIM system identifier',
  tag_identifier                  varchar(30)       DEFAULT NULL                                                   COMMENT 'Tag index within tag set, NULL if untagged',
  tag_sequence                    varchar(30)       DEFAULT NULL                                                   COMMENT 'Tag sequence for tag',
  tag_set_id_lims                 varchar(20)       DEFAULT NULL                                                   COMMENT 'LIMs-specific identifier of the tag set for tag',
  tag_set_name                    varchar(100)      DEFAULT NULL                                                   COMMENT 'WTSI-wide tag set name for tag',
  tag2_sequence                   varchar(30)       DEFAULT NULL,
  tag2_set_id_lims                varchar(20)       DEFAULT NULL,
  tag2_set_name                   varchar(100)      DEFAULT NULL,
  tag2_identifier                 varchar(30)       DEFAULT NULL,
  plate_barcode                   varchar(255)      DEFAULT NULL                                                   COMMENT 'The human readable barcode for the plate loaded onto the machine',
  plate_uuid_lims                 varchar(36)       NOT NULL                                                       COMMENT 'The plate uuid',
  well_label                      varchar(255)      NOT NULL                                                       COMMENT 'The well identifier for the plate, A1-H12',
  well_uuid_lims                  varchar(36)       NOT NULL                                                       COMMENT 'The well uuid',
  pac_bio_library_tube_id_lims    varchar(255)      NOT NULL                                                       COMMENT 'LIMS specific identifier for originating library tube',
  pac_bio_library_tube_uuid       varchar(255)      NOT NULL                                                       COMMENT 'The uuid for the originating library tube',
  pac_bio_library_tube_name       varchar(255)      NOT NULL                                                       COMMENT 'The name of the originating library tube',
  pac_bio_library_tube_legacy_id  int(11)           DEFAULT NULL                                                   COMMENT 'Legacy library_id for backwards compatibility.',
  library_created_at              datetime          DEFAULT NULL                                                   COMMENT 'Timestamp of library creation',
  pac_bio_run_name                varchar(255)      DEFAULT NULL                                                   COMMENT 'Name of the run',
  pipeline_id_lims                varchar(60)       DEFAULT NULL                                                   COMMENT 'LIMS-specific pipeline identifier that unambiguously defines library type (eg. Sequel-v1, IsoSeq-v1)',
  comparable_tag_identifier       varchar(255)      GENERATED ALWAYS AS (ifnull(`tag_identifier`,-(1))) VIRTUAL,
  comparable_tag2_identifier      varchar(255)      GENERATED ALWAYS AS (ifnull(`tag2_identifier`,-(1))) VIRTUAL,
  plate_number                    int(11)           DEFAULT NULL                                                   COMMENT 'The number of the plate that goes onto the sequencing machine. Necessary as an identifier for multi-plate support.',
  pac_bio_library_tube_barcode    varchar(255)      DEFAULT NULL                                                   COMMENT 'The barcode of the originating library tube',

  PRIMARY KEY (id_pac_bio_tmp),
  UNIQUE KEY unique_pac_bio_entry (id_lims,id_pac_bio_run_lims,well_label,comparable_tag_identifier,comparable_tag2_identifier),
  KEY fk_pac_bio_run_to_sample (id_sample_tmp),
  KEY fk_pac_bio_run_to_study (id_study_tmp),
  CONSTRAINT fk_pac_bio_run_to_sample FOREIGN KEY (id_sample_tmp) REFERENCES sample (id_sample_tmp),
  CONSTRAINT fk_pac_bio_run_to_study FOREIGN KEY (id_study_tmp) REFERENCES study (id_study_tmp)
) ENGINE=InnoDB AUTO_INCREMENT=108139 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



CREATE TABLE pac_bio_run_well_metrics (

  id_pac_bio_rw_metrics_tmp             int(11)               NOT NULL AUTO_INCREMENT,
  id_pac_bio_product                    char(64)              CHARACTER SET utf8 NOT NULL      COMMENT 'Product id',
  pac_bio_run_name                      varchar(255)          CHARACTER SET utf8 NOT NULL      COMMENT 'Lims specific identifier for the pacbio run',
  well_label                            varchar(255)          CHARACTER SET utf8 NOT NULL      COMMENT 'The well identifier for the plate, A1-H12',
  qc_seq_state                          varchar(255)          DEFAULT NULL                     COMMENT 'Current sequencing QC state',
  qc_seq_state_is_final                 tinyint(1)            DEFAULT NULL                     COMMENT 'A flag marking the sequencing QC state as final (1) or not final (0)',
  qc_seq_date                           datetime              DEFAULT NULL                     COMMENT 'The date the current sequencing QC state was assigned',
  qc_seq                                tinyint(1)            DEFAULT NULL                     COMMENT 'The final sequencing QC outcome as 0(failed), 1(passed) or NULL',
  instrument_type                       varchar(32)           CHARACTER SET utf8 NOT NULL      COMMENT 'The instrument type e.g. Sequel',
  instrument_name                       varchar(32)           CHARACTER SET utf8 DEFAULT NULL  COMMENT 'The instrument name e.g. SQ54097',
  chip_type                             varchar(32)           CHARACTER SET utf8 DEFAULT NULL  COMMENT 'The chip type e.g. 8mChip',
  sl_hostname                           varchar(255)          CHARACTER SET utf8 DEFAULT NULL  COMMENT 'SMRT Link server hostname',
  sl_run_uuid                           varchar(36)           CHARACTER SET utf8 DEFAULT NULL  COMMENT 'SMRT Link specific run uuid',
  sl_ccs_uuid                           varchar(36)           CHARACTER SET utf8 DEFAULT NULL  COMMENT 'SMRT Link specific ccs dataset uuid',
  ts_run_name                           varchar(32)           CHARACTER SET utf8 DEFAULT NULL  COMMENT 'The PacBio run name',
  movie_name                            varchar(32)           CHARACTER SET utf8 DEFAULT NULL  COMMENT 'The PacBio movie name',
  movie_minutes                         smallint(5) unsigned  DEFAULT NULL                     COMMENT 'Movie time (collection time) in minutes',
  created_by                            varchar(32)           CHARACTER SET utf8 DEFAULT NULL  COMMENT 'Created by user name recorded in SMRT Link',
  binding_kit                           varchar(255)          CHARACTER SET utf8 DEFAULT NULL  COMMENT 'Binding kit version',
  sequencing_kit                        varchar(255)          CHARACTER SET utf8 DEFAULT NULL  COMMENT 'Sequencing kit version',
  sequencing_kit_lot_number             varchar(255)          CHARACTER SET utf8 DEFAULT NULL  COMMENT 'Sequencing Kit lot number',
  cell_lot_number                       varchar(32)           DEFAULT NULL                     COMMENT 'SMRT Cell Lot Number',
  ccs_execution_mode                    varchar(32)           CHARACTER SET utf8 DEFAULT NULL  COMMENT 'The PacBio ccs exection mode e.g. OnInstument, OffInstument or None',
  demultiplex_mode                      varchar(32)           CHARACTER SET utf8 DEFAULT NULL  COMMENT 'Demultiplexing mode e.g. OnInstument, OffInstument or None',
  include_kinetics                      tinyint(1) unsigned   DEFAULT NULL                     COMMENT 'Include kinetics information where ccs is run',
  hifi_only_reads                       tinyint(1) unsigned   DEFAULT NULL                     COMMENT 'CCS was run on the instrument and only HiFi reads were included in the export from the instrument',
  heteroduplex_analysis                 tinyint(1) unsigned   DEFAULT NULL                     COMMENT 'Analysis has been run on the instrument to detect and resolve heteroduplex reads',
  loading_conc                          float unsigned        DEFAULT NULL                     COMMENT 'SMRT Cell loading concentration (pM)',
  run_start                             datetime              DEFAULT NULL                     COMMENT 'Timestamp of run started',
  run_complete                          datetime              DEFAULT NULL                     COMMENT 'Timestamp of run complete',
  run_transfer_complete                 datetime              DEFAULT NULL                     COMMENT 'Timestamp of run transfer complete',
  run_status                            varchar(32)           DEFAULT NULL                     COMMENT 'Last recorded status, primarily to explain runs not completed.',
  well_start                            datetime              DEFAULT NULL                     COMMENT 'Timestamp of well started',
  well_complete                         datetime              DEFAULT NULL                     COMMENT 'Timestamp of well complete',
  well_status                           varchar(32)           DEFAULT NULL                     COMMENT 'Last recorded status, primarily to explain wells not completed.',
  chemistry_sw_version                  varchar(32)           CHARACTER SET utf8 DEFAULT NULL  COMMENT 'The PacBio chemistry software version',
  instrument_sw_version                 varchar(32)           CHARACTER SET utf8 DEFAULT NULL  COMMENT 'The PacBio instrument software version',
  primary_analysis_sw_version           varchar(32)           CHARACTER SET utf8 DEFAULT NULL  COMMENT 'The PacBio primary analysis software version',
  control_num_reads                     int(10) unsigned      DEFAULT NULL                     COMMENT 'The number of control reads',
  control_concordance_mean              float(8,6) unsigned   DEFAULT NULL                     COMMENT 'The average concordance between the control raw reads and the control reference sequence',
  control_concordance_mode              float unsigned        DEFAULT NULL                     COMMENT 'The modal value from the concordance between the control raw reads and the control reference sequence',
  control_read_length_mean              int(10) unsigned      DEFAULT NULL                     COMMENT 'The mean polymerase read length of the control reads',
  local_base_rate                       float(8,6) unsigned   DEFAULT NULL                     COMMENT 'The average base incorporation rate, excluding polymerase pausing events',
  polymerase_read_bases                 bigint(20) unsigned   DEFAULT NULL                     COMMENT 'Calculated by multiplying the number of productive (P1) ZMWs by the mean polymerase read length',
  polymerase_num_reads                  int(10) unsigned      DEFAULT NULL                     COMMENT 'The number of polymerase reads',
  polymerase_read_length_mean           int(10) unsigned      DEFAULT NULL                     COMMENT 'The mean high-quality read length of all polymerase reads',
  polymerase_read_length_n50            int(10) unsigned      DEFAULT NULL                     COMMENT 'Fifty percent of the trimmed read length of all polymerase reads are longer than this value',
  insert_length_mean                    int(10) unsigned      DEFAULT NULL                     COMMENT 'The average subread length, considering only the longest subread from each ZMW',
  insert_length_n50                     int(10) unsigned      DEFAULT NULL                     COMMENT 'Fifty percent of the subreads are longer than this value when considering only the longest subread from each ZMW',
  unique_molecular_bases                bigint(20) unsigned   DEFAULT NULL                     COMMENT 'The unique molecular yield in bp',
  productive_zmws_num                   int(10) unsigned      DEFAULT NULL                     COMMENT 'Number of productive ZMWs',
  p0_num                                int(10) unsigned      DEFAULT NULL                     COMMENT 'Number of empty ZMWs with no high quality read detected',
  p1_num                                int(10) unsigned      DEFAULT NULL                     COMMENT 'Number of ZMWs with a high quality read detected',
  p2_num                                int(10) unsigned      DEFAULT NULL                     COMMENT 'Number of other ZMWs, signal detected but no high quality read',
  adapter_dimer_percent                 float(5,2) unsigned   DEFAULT NULL                     COMMENT 'The percentage of pre-filter ZMWs which have observed inserts of 0-10 bp',
  short_insert_percent                  float(5,2) unsigned   DEFAULT NULL                     COMMENT 'The percentage of pre-filter ZMWs which have observed inserts of 11-100 bp',
  hifi_read_bases                       bigint(20) unsigned   DEFAULT NULL                     COMMENT 'The number of HiFi bases',
  hifi_num_reads                        int(10) unsigned      DEFAULT NULL                     COMMENT 'The number of HiFi reads',
  hifi_read_length_mean                 int(10) unsigned      DEFAULT NULL                     COMMENT 'The mean HiFi read length',
  hifi_read_quality_median              smallint(5) unsigned  DEFAULT NULL                     COMMENT 'The median HiFi base quality',
  hifi_number_passes_mean               int(10) unsigned      DEFAULT NULL                     COMMENT 'The mean number of passes per HiFi read',
  hifi_low_quality_read_bases           bigint(20) unsigned   DEFAULT NULL                     COMMENT 'The number of HiFi bases filtered due to low quality (<Q20)',
  hifi_low_quality_num_reads            int(10) unsigned      DEFAULT NULL                     COMMENT 'The number of HiFi reads filtered due to low quality (<Q20)',
  hifi_low_quality_read_length_mean     int(10) unsigned      DEFAULT NULL                     COMMENT 'The mean length of HiFi reads filtered due to low quality (<Q20)',
  hifi_low_quality_read_quality_median  smallint(5) unsigned  DEFAULT NULL                     COMMENT 'The median base quality of HiFi bases filtered due to low quality (<Q20)',
  hifi_barcoded_reads                   int(10) unsigned      DEFAULT NULL                     COMMENT 'Number of reads with an expected barcode in demultiplexed HiFi data',
  hifi_bases_in_barcoded_reads          bigint(20) unsigned   DEFAULT NULL                     COMMENT 'Number of bases in reads with an expected barcode in demultiplexed HiFi data',

  PRIMARY KEY (id_pac_bio_rw_metrics_tmp),
  UNIQUE KEY pac_bio_metrics_run_well (pac_bio_run_name,well_label),
  UNIQUE KEY pac_bio_rw_metrics_id_product (id_pac_bio_product),
  KEY pbrw_movie_name_index (movie_name),
  KEY pbrw_ccs_execmode_index (ccs_execution_mode),
  KEY pbrw_run_complete_index (run_complete),
  KEY pbrw_well_complete_index (well_complete),
  KEY pb_rw_qc_state_index (qc_seq_state,qc_seq_state_is_final),
  KEY pb_rw_qc_date_index (qc_seq_date)
) ENGINE=InnoDB AUTO_INCREMENT=4234 DEFAULT CHARSET=utf8 COMMENT='Status and run information by well and some basic QC data from SMRT Link';



CREATE TABLE psd_sample_compounds_components (

  id                       bigint(20)  NOT NULL AUTO_INCREMENT,
  compound_id_sample_tmp   int(11)     NOT NULL                  COMMENT 'The warehouse ID of the compound sample in the association.',
  component_id_sample_tmp  int(11)     NOT NULL                  COMMENT 'The warehouse ID of the component sample in the association.',
  last_updated             datetime    NOT NULL                  COMMENT 'Timestamp of last update.',
  recorded_at              datetime    NOT NULL                  COMMENT 'Timestamp of warehouse update.',

  PRIMARY KEY (id)
) ENGINE=InnoDB AUTO_INCREMENT=8279 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci COMMENT='A join table owned by PSD to associate compound samples with their component samples.';



CREATE TABLE qc_result (

  id_qc_result_tmp   int(11)           NOT NULL AUTO_INCREMENT,
  id_sample_tmp      int(10) unsigned  NOT NULL,
  id_qc_result_lims  varchar(20)       NOT NULL                  COMMENT 'LIMS-specific qc_result identifier',
  id_lims            varchar(10)       NOT NULL                  COMMENT 'LIMS system identifier (e.g. SEQUENCESCAPE)',
  id_pool_lims       varchar(255)      DEFAULT NULL              COMMENT 'Most specific LIMs identifier associated with the pool. (Asset external_identifier in SS)',
  id_library_lims    varchar(255)      DEFAULT NULL              COMMENT 'Earliest LIMs identifier associated with library creation. (Aliquot external_identifier in SS)',
  labware_purpose    varchar(255)      DEFAULT NULL              COMMENT 'Labware Purpose name. (e.g. Plate Purpose for a Well)',
  assay              varchar(255)      DEFAULT NULL              COMMENT 'assay type and version',
  `value`            varchar(255)      NOT NULL                  COMMENT 'Value of the mesurement',
  units              varchar(255)      NOT NULL                  COMMENT 'Mesurement unit',
  cv                 float             DEFAULT NULL              COMMENT 'Coefficient of variance',
  qc_type            varchar(255)      NOT NULL                  COMMENT 'Type of mesurement',
  date_created       datetime          NOT NULL                  COMMENT 'The date the qc_result was first created in SS',
  last_updated       datetime          NOT NULL                  COMMENT 'The date the qc_result was last updated in SS',
  recorded_at        datetime          NOT NULL                  COMMENT 'Timestamp of warehouse update',

  PRIMARY KEY (id_qc_result_tmp),
  KEY fk_qc_result_to_sample (id_sample_tmp),
  KEY lookup_index (id_qc_result_lims,id_lims),
  KEY qc_result_id_library_lims_index (id_library_lims),
  CONSTRAINT fk_qc_result_to_sample FOREIGN KEY (id_sample_tmp) REFERENCES sample (id_sample_tmp)
) ENGINE=InnoDB AUTO_INCREMENT=19267008 DEFAULT CHARSET=latin1;



CREATE TABLE sample (

  id_sample_tmp                    int(10) unsigned  NOT NULL AUTO_INCREMENT   COMMENT 'Internal to this database id, value can change',
  id_lims                          varchar(10)       NOT NULL                  COMMENT 'LIM system identifier, e.g. CLARITY-GCLP, SEQSCAPE',
  uuid_sample_lims                 varchar(36)       DEFAULT NULL              COMMENT 'LIMS-specific sample uuid',
  id_sample_lims                   varchar(20)       NOT NULL                  COMMENT 'LIMS-specific sample identifier',
  last_updated                     datetime          NOT NULL                  COMMENT 'Timestamp of last update',
  recorded_at                      datetime          NOT NULL                  COMMENT 'Timestamp of warehouse update',
  deleted_at                       datetime          DEFAULT NULL              COMMENT 'Timestamp of sample deletion',
  created                          datetime          DEFAULT NULL              COMMENT 'Timestamp of sample creation',
  `name`                           varchar(255)      DEFAULT NULL,
  reference_genome                 varchar(255)      DEFAULT NULL,
  organism                         varchar(255)      DEFAULT NULL,
  accession_number                 varchar(255)      DEFAULT NULL              COMMENT 'A unique identifier generated by the INSDC',
  common_name                      varchar(255)      DEFAULT NULL,
  description                      text              COLLATE utf8_unicode_ci,
  taxon_id                         int(6) unsigned   DEFAULT NULL,
  father                           varchar(255)      DEFAULT NULL,
  mother                           varchar(255)      DEFAULT NULL,
  replicate                        varchar(255)      DEFAULT NULL,
  ethnicity                        varchar(255)      DEFAULT NULL,
  gender                           varchar(20)       DEFAULT NULL,
  cohort                           varchar(255)      DEFAULT NULL,
  country_of_origin                varchar(255)      DEFAULT NULL,
  geographical_region              varchar(255)      DEFAULT NULL,
  sanger_sample_id                 varchar(255)      DEFAULT NULL,
  control                          tinyint(1)        DEFAULT NULL,
  supplier_name                    varchar(255)      DEFAULT NULL,
  public_name                      varchar(255)      DEFAULT NULL,
  sample_visibility                varchar(255)      DEFAULT NULL,
  strain                           varchar(255)      DEFAULT NULL,
  consent_withdrawn                tinyint(1)        NOT NULL DEFAULT '0',
  donor_id                         varchar(255)      DEFAULT NULL,
  phenotype                        varchar(255)      DEFAULT NULL              COMMENT 'The phenotype of the sample as described in Sequencescape',
  developmental_stage              varchar(255)      DEFAULT NULL              COMMENT 'Developmental Stage',
  control_type                     varchar(255)      DEFAULT NULL,
  sibling                          varchar(255)      DEFAULT NULL,
  is_resubmitted                   tinyint(1)        DEFAULT NULL,
  date_of_sample_collection        varchar(255)      DEFAULT NULL,
  date_of_sample_extraction        varchar(255)      DEFAULT NULL,
  extraction_method                varchar(255)      DEFAULT NULL,
  purified                         varchar(255)      DEFAULT NULL,
  purification_method              varchar(255)      DEFAULT NULL,
  customer_measured_concentration  varchar(255)      DEFAULT NULL,
  concentration_determined_by      varchar(255)      DEFAULT NULL,
  sample_type                      varchar(255)      DEFAULT NULL,
  storage_conditions               varchar(255)      DEFAULT NULL,
  genotype                         varchar(255)      DEFAULT NULL,
  age                              varchar(255)      DEFAULT NULL,
  cell_type                        varchar(255)      DEFAULT NULL,
  disease_state                    varchar(255)      DEFAULT NULL,
  compound                         varchar(255)      DEFAULT NULL,
  dose                             varchar(255)      DEFAULT NULL,
  immunoprecipitate                varchar(255)      DEFAULT NULL,
  growth_condition                 varchar(255)      DEFAULT NULL,
  organism_part                    varchar(255)      DEFAULT NULL,
  time_point                       varchar(255)      DEFAULT NULL,
  disease                          varchar(255)      DEFAULT NULL,
  `subject`                        varchar(255)      DEFAULT NULL,
  treatment                        varchar(255)      DEFAULT NULL,
  date_of_consent_withdrawn        datetime          DEFAULT NULL,
  marked_as_consent_withdrawn_by   varchar(255)      DEFAULT NULL,
  customer_measured_volume         varchar(255)      DEFAULT NULL,
  gc_content                       varchar(255)      DEFAULT NULL,
  dna_source                       varchar(255)      DEFAULT NULL,

  PRIMARY KEY (id_sample_tmp),
  UNIQUE KEY index_sample_on_id_sample_lims_and_id_lims (id_sample_lims,id_lims),
  UNIQUE KEY sample_uuid_sample_lims_index (uuid_sample_lims),
  KEY sample_accession_number_index (accession_number),
  KEY sample_name_index (`name`),
  KEY index_sample_on_supplier_name (supplier_name),
  KEY index_sample_on_sanger_sample_id (sanger_sample_id)
) ENGINE=InnoDB AUTO_INCREMENT=8630453 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



CREATE TABLE samples_extraction_activity (

  id_activity_tmp   int(11)           NOT NULL AUTO_INCREMENT,
  id_activity_lims  varchar(255)      NOT NULL                  COMMENT 'LIMs-specific activity id',
  id_sample_tmp     int(10) unsigned  NOT NULL                  COMMENT 'Sample id, see "sample.id_sample_tmp"',
  activity_type     varchar(255)      NOT NULL                  COMMENT 'The type of the activity performed',
  instrument        varchar(255)      NOT NULL                  COMMENT 'The name of the instrument used to perform the activity',
  kit_barcode       varchar(255)      NOT NULL                  COMMENT 'The barcode of the kit used to perform the activity',
  kit_type          varchar(255)      NOT NULL                  COMMENT 'The type of kit used to perform the activity',
  input_barcode     varchar(255)      NOT NULL                  COMMENT 'The barcode of the labware (eg. plate or tube) at the begining of the activity',
  output_barcode    varchar(255)      NOT NULL                  COMMENT 'The barcode of the labware (eg. plate or tube)  at the end of the activity',
  `user`            varchar(255)      NOT NULL                  COMMENT 'The name of the user who was most recently associated with the activity',
  last_updated      datetime          NOT NULL                  COMMENT 'Timestamp of last change to activity',
  recorded_at       datetime          NOT NULL                  COMMENT 'Timestamp of warehouse update',
  completed_at      datetime          NOT NULL                  COMMENT 'Timestamp of activity completion',
  deleted_at        datetime          DEFAULT NULL              COMMENT 'Timestamp of any activity removal',
  id_lims           varchar(10)       NOT NULL                  COMMENT 'LIM system identifier',

  PRIMARY KEY (id_activity_tmp),
  KEY index_samples_extraction_activity_on_id_activity_lims (id_activity_lims),
  KEY fk_rails_bbdd0468f0 (id_sample_tmp),
  CONSTRAINT fk_rails_bbdd0468f0 FOREIGN KEY (id_sample_tmp) REFERENCES sample (id_sample_tmp)
) ENGINE=InnoDB AUTO_INCREMENT=61163 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



CREATE TABLE schema_migrations (

  version  varchar(255)  NOT NULL,

  UNIQUE KEY unique_schema_migrations (version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



CREATE TABLE seq_product_irods_locations (

  id_seq_product_irods_locations_tmp  bigint(20) unsigned  NOT NULL AUTO_INCREMENT                                COMMENT 'Internal to this database id, value can change',
  created                             datetime             DEFAULT CURRENT_TIMESTAMP                              COMMENT 'Datetime this record was created',
  last_changed                        datetime             DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP  COMMENT 'Datetime this record was created or changed',
  id_product                          varchar(64)          CHARACTER SET utf8 NOT NULL                            COMMENT 'A sequencing platform specific product id. For Illumina, data corresponds to the id_iseq_product column in the iseq_product_metrics table',
  seq_platform_name                   enum('               Illumina','PacBio','ONT') NOT NULL                     COMMENT 'Name of the sequencing platform used to produce raw data',
  pipeline_name                       varchar(32)          NOT NULL                                               COMMENT 'The name of the pipeline used to produce the data, values are: npg-prod, npg-prod-alt-process, cellranger, spaceranger, ncov2019-artic-nf',
  irods_root_collection               varchar(255)         NOT NULL                                               COMMENT 'Path to the product root collection in iRODS',
  irods_data_relative_path            varchar(255)         DEFAULT NULL                                           COMMENT 'The path, relative to the root collection, to the most used data location',
  irods_secondary_data_relative_path  varchar(255)         DEFAULT NULL                                           COMMENT 'The path, relative to the root collection, to a useful data location',

  PRIMARY KEY (id_seq_product_irods_locations_tmp),
  UNIQUE KEY pi_root_product (irods_root_collection,id_product),
  KEY pi_id_product (id_product),
  KEY pi_seq_platform_name (seq_platform_name),
  KEY pi_pipeline_name (pipeline_name)
) ENGINE=InnoDB AUTO_INCREMENT=6505389 DEFAULT CHARSET=utf8 COMMENT='Table relating products to their irods locations';



CREATE TABLE stock_resource (

  id_stock_resource_tmp    int(11)           NOT NULL AUTO_INCREMENT,
  last_updated             datetime          NOT NULL                  COMMENT 'Timestamp of last update',
  recorded_at              datetime          NOT NULL                  COMMENT 'Timestamp of warehouse update',
  created                  datetime          NOT NULL                  COMMENT 'Timestamp of initial registration of stock in LIMS',
  deleted_at               datetime          DEFAULT NULL              COMMENT 'Timestamp of initial registration of deletion in parent LIMS. NULL if not deleted.',
  id_sample_tmp            int(10) unsigned  NOT NULL                  COMMENT 'Sample id, see "sample.id_sample_tmp"',
  id_study_tmp             int(10) unsigned  NOT NULL                  COMMENT 'Sample id, see "study.id_study_tmp"',
  id_lims                  varchar(10)       NOT NULL                  COMMENT 'LIM system identifier',
  id_stock_resource_lims   varchar(20)       NOT NULL                  COMMENT 'Lims specific identifier for the stock',
  stock_resource_uuid      varchar(36)       DEFAULT NULL              COMMENT 'Uuid identifier for the stock',
  labware_type             varchar(255)      NOT NULL                  COMMENT 'The type of labware containing the stock. eg. Well, Tube',
  labware_machine_barcode  varchar(255)      NOT NULL                  COMMENT 'The barcode of the containing labware as read by a barcode scanner',
  labware_human_barcode    varchar(255)      NOT NULL                  COMMENT 'The barcode of the containing labware in human readable format',
  labware_coordinate       varchar(255)      DEFAULT NULL              COMMENT 'For wells, the coordinate on the containing plate. Null for tubes.',
  current_volume           float             DEFAULT NULL              COMMENT 'The current volume of material in microlitres based on measurements and know usage',
  initial_volume           float             DEFAULT NULL              COMMENT 'The result of the initial volume measurement in microlitres conducted on the material',
  concentration            float             DEFAULT NULL              COMMENT 'The concentration of material recorded in the lab in nanograms per microlitre',
  gel_pass                 varchar(255)      DEFAULT NULL              COMMENT 'The recorded result for the qel QC assay.',
  pico_pass                varchar(255)      DEFAULT NULL              COMMENT 'The recorded result for the pico green assay. A pass indicates a successful assay, not sufficient material.',
  snp_count                int(11)           DEFAULT NULL              COMMENT 'The number of markers detected in genotyping assays',
  measured_gender          varchar(255)      DEFAULT NULL              COMMENT 'The gender call base on the genotyping assay',

  PRIMARY KEY (id_stock_resource_tmp),
  KEY fk_stock_resource_to_sample (id_sample_tmp),
  KEY fk_stock_resource_to_study (id_study_tmp),
  KEY composition_lookup_index (id_stock_resource_lims,id_sample_tmp,id_lims),
  KEY index_stock_resource_on_labware_human_barcode (labware_human_barcode),
  CONSTRAINT fk_stock_resource_to_sample FOREIGN KEY (id_sample_tmp) REFERENCES sample (id_sample_tmp),
  CONSTRAINT fk_stock_resource_to_study FOREIGN KEY (id_study_tmp) REFERENCES study (id_study_tmp)
) ENGINE=InnoDB AUTO_INCREMENT=8429246 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



CREATE TABLE study (

  id_study_tmp                    int(10) unsigned  NOT NULL AUTO_INCREMENT   COMMENT 'Internal to this database id, value can change',
  id_lims                         varchar(10)       NOT NULL                  COMMENT 'LIM system identifier, e.g. GCLP-CLARITY, SEQSCAPE',
  uuid_study_lims                 varchar(36)       DEFAULT NULL              COMMENT 'LIMS-specific study uuid',
  id_study_lims                   varchar(20)       NOT NULL                  COMMENT 'LIMS-specific study identifier',
  last_updated                    datetime          NOT NULL                  COMMENT 'Timestamp of last update',
  recorded_at                     datetime          NOT NULL                  COMMENT 'Timestamp of warehouse update',
  deleted_at                      datetime          DEFAULT NULL              COMMENT 'Timestamp of study deletion',
  created                         datetime          DEFAULT NULL              COMMENT 'Timestamp of study creation',
  `name`                          varchar(255)      DEFAULT NULL,
  reference_genome                varchar(255)      DEFAULT NULL,
  ethically_approved              tinyint(1)        DEFAULT NULL,
  faculty_sponsor                 varchar(255)      DEFAULT NULL,
  state                           varchar(50)       DEFAULT NULL,
  study_type                      varchar(50)       DEFAULT NULL,
  abstract                        text              COLLATE utf8_unicode_ci,
  abbreviation                    varchar(255)      DEFAULT NULL,
  accession_number                varchar(50)       DEFAULT NULL,
  description                     text              COLLATE utf8_unicode_ci,
  contains_human_dna              tinyint(1)        DEFAULT NULL              COMMENT 'Lane may contain human DNA',
  contaminated_human_dna          tinyint(1)        DEFAULT NULL              COMMENT 'Human DNA in the lane is a contaminant and should be removed',
  data_release_strategy           varchar(255)      DEFAULT NULL,
  data_release_sort_of_study      varchar(255)      DEFAULT NULL,
  ena_project_id                  varchar(255)      DEFAULT NULL,
  study_title                     varchar(255)      DEFAULT NULL,
  study_visibility                varchar(255)      DEFAULT NULL,
  ega_dac_accession_number        varchar(255)      DEFAULT NULL,
  array_express_accession_number  varchar(255)      DEFAULT NULL,
  ega_policy_accession_number     varchar(255)      DEFAULT NULL,
  data_release_timing             varchar(255)      DEFAULT NULL,
  data_release_delay_period       varchar(255)      DEFAULT NULL,
  data_release_delay_reason       varchar(255)      DEFAULT NULL,
  remove_x_and_autosomes          tinyint(1)        NOT NULL DEFAULT '0',
  aligned                         tinyint(1)        NOT NULL DEFAULT '1',
  separate_y_chromosome_data      tinyint(1)        NOT NULL DEFAULT '0',
  data_access_group               varchar(255)      DEFAULT NULL,
  prelim_id                       varchar(20)       DEFAULT NULL              COMMENT 'The preliminary study id prior to entry into the LIMS',
  hmdmc_number                    varchar(255)      DEFAULT NULL              COMMENT 'The Human Materials and Data Management Committee approval number(s) for the study.',
  data_destination                varchar(255)      DEFAULT NULL              COMMENT 'The data destination type(s) for the study. It could be ''standard'', ''14mg'' or ''gseq''. This may be extended, if Sanger gains more external customers. It can contain multiply destinations separated by a space.',
  s3_email_list                   varchar(255)      DEFAULT NULL,
  data_deletion_period            varchar(255)      DEFAULT NULL,

  PRIMARY KEY (id_study_tmp),
  UNIQUE KEY study_id_lims_id_study_lims_index (id_lims,id_study_lims),
  UNIQUE KEY study_uuid_study_lims_index (uuid_study_lims),
  KEY study_accession_number_index (accession_number),
  KEY study_name_index (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=7287 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



CREATE TABLE study_users (

  id_study_users_tmp  int(10) unsigned  NOT NULL AUTO_INCREMENT  COMMENT 'Internal to this database id, value can change',
  id_study_tmp        int(10) unsigned  NOT NULL                 COMMENT 'Study id, see "study.id_study_tmp"',
  last_updated        datetime          NOT NULL                 COMMENT 'Timestamp of last update',
  role                varchar(255)      DEFAULT NULL,
  login               varchar(255)      DEFAULT NULL,
  email               varchar(255)      DEFAULT NULL,
  `name`              varchar(255)      DEFAULT NULL,

  PRIMARY KEY (id_study_users_tmp),
  KEY study_users_study_fk (id_study_tmp),
  CONSTRAINT study_users_study_fk FOREIGN KEY (id_study_tmp) REFERENCES study (id_study_tmp) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=679226 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



CREATE TABLE tol_sample_bioproject (

  id_tsb_tmp            int(10) unsigned  NOT NULL AUTO_INCREMENT,
  id_sample_tmp         int(10) unsigned  DEFAULT NULL,
  `file`                varchar(255)      DEFAULT NULL,
  library_type          enum('            Chromium genome','Haplotagging','Hi-C','Hi-C - Arima v1','Hi-C - Arima v2','Hi-C - Dovetail','Hi-C - Omni-C','Hi-C - Qiagen','PacBio - CLR','PacBio - HiFi','ONT','RNA PolyA','RNA-seq dUTP eukaryotic','Standard','unknown','HiSeqX PCR free','PacBio - HiFi (ULI)','PacBio - IsoSeq') DEFAULT NULL,
  tolid                 varchar(40)       DEFAULT NULL,
  biosample_accession   varchar(255)      DEFAULT NULL,
  bioproject_accession  varchar(255)      DEFAULT NULL,
  date_added            timestamp         NOT NULL DEFAULT CURRENT_TIMESTAMP,
  date_updated          timestamp         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  filename              varchar(255)      DEFAULT NULL,

  PRIMARY KEY (id_tsb_tmp),
  UNIQUE KEY tol_sample_bioproject_file_index (`file`),
  KEY fk_tsb_to_sample (id_sample_tmp),
  CONSTRAINT fk_tsb_to_sample FOREIGN KEY (id_sample_tmp) REFERENCES sample (id_sample_tmp) ON DELETE SET NULL
) ENGINE=InnoDB AUTO_INCREMENT=5655 DEFAULT CHARSET=utf8;



























