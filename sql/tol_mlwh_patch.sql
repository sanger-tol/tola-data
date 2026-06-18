
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;

ALTER TABLE tol_sample_bioproject CHANGE COLUMN id_tsb_tmp id_tsb_tmp int unsigned NOT NULL AUTO_INCREMENT
COMMENT 'Internal primary key';

ALTER TABLE tol_sample_bioproject CHANGE COLUMN id_sample_tmp id_sample_tmp int unsigned DEFAULT NULL
COMMENT 'Sample ID, see `sample.id_sample_tmp`';

ALTER TABLE tol_sample_bioproject CHANGE COLUMN `file` `file` varchar(500) DEFAULT NULL
COMMENT 'Path to the source file';

ALTER TABLE tol_sample_bioproject CHANGE COLUMN filename filename varchar(255) DEFAULT NULL
COMMENT 'Name which should be given to the submitted file' AFTER file;

ALTER TABLE tol_sample_bioproject ADD COLUMN platform varchar(40) DEFAULT NULL
COMMENT 'ENA PLATFORM metadata field. Not required if INSTRUMENT is provided' AFTER filename;

ALTER TABLE tol_sample_bioproject ADD COLUMN instrument varchar(40) DEFAULT NULL
COMMENT 'ENA INSTRUMENT metadata field' AFTER platform;

ALTER TABLE tol_sample_bioproject ADD COLUMN library_name varchar(40) DEFAULT NULL
COMMENT 'ENA LIBRARY_NAME metadata field' AFTER instrument;

ALTER TABLE tol_sample_bioproject ADD COLUMN library_source varchar(40) DEFAULT NULL
COMMENT 'ENA LIBRARY_SOURCE metadata field' AFTER library_name;

ALTER TABLE tol_sample_bioproject ADD COLUMN library_selection varchar(40) DEFAULT NULL
COMMENT 'ENA LIBRARY_SELECTION metadata field' AFTER library_source;

ALTER TABLE tol_sample_bioproject ADD COLUMN library_strategy varchar(40) DEFAULT NULL
COMMENT 'ENA LIBRARY_STRATEGY metadata field' AFTER library_selection;

ALTER TABLE tol_sample_bioproject CHANGE COLUMN library_type library_type varchar(40) DEFAULT NULL
COMMENT 'Temporary home for ENA LIBRARY_CONSTRUCTION_PROTOCOL metadata field during migration to library_construction_protocol';

ALTER TABLE tol_sample_bioproject ADD COLUMN library_construction_protocol varchar(40) DEFAULT NULL
COMMENT 'ENA LIBRARY_CONSTRUCTION_PROTOCOL metadata field' AFTER library_type;

ALTER TABLE tol_sample_bioproject ADD COLUMN design_description varchar(500) DEFAULT NULL
COMMENT 'ENA DESCRIPTION metadata field. Free text description of the library' AFTER library_construction_protocol;

ALTER TABLE tol_sample_bioproject CHANGE COLUMN tolid tolid varchar(40) DEFAULT NULL
COMMENT 'Tree of Life ID, see: https://id.tol.sanger.ac.uk';

ALTER TABLE tol_sample_bioproject ADD COLUMN data_id varchar(128) DEFAULT NULL
COMMENT 'Value from ToLQC database `data.data_id`' AFTER tolid;

ALTER TABLE tol_sample_bioproject CHANGE COLUMN biosample_accession biosample_accession varchar(255) DEFAULT NULL
COMMENT 'ENA biosample accession for the sample';

ALTER TABLE tol_sample_bioproject CHANGE COLUMN bioproject_accession bioproject_accession varchar(255) DEFAULT NULL
COMMENT 'ENA project data accession for the species';

ALTER TABLE tol_sample_bioproject CHANGE COLUMN date_added date_added timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
COMMENT 'When row was created';

ALTER TABLE tol_sample_bioproject CHANGE COLUMN date_updated date_updated timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
COMMENT 'When row was last updated';

SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
