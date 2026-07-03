
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;


-- Alter the table structure:

ALTER TABLE tol_sample_bioproject DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_unicode_ci;

ALTER TABLE tol_sample_bioproject CHANGE COLUMN id_tsb_tmp id_tsb_tmp int unsigned NOT NULL AUTO_INCREMENT
COMMENT 'Internal primary key';

ALTER TABLE tol_sample_bioproject ADD COLUMN data_id varchar(128)
COMMENT 'Value from ToLQC database `data.data_id`' AFTER id_tsb_tmp;

ALTER TABLE tol_sample_bioproject CHANGE COLUMN id_sample_tmp id_sample_tmp int unsigned
COMMENT 'Link to "temporary" `sample.id_sample_tmp`';

ALTER TABLE tol_sample_bioproject ADD COLUMN uuid_sample_lims varchar(36)
COMMENT 'Link to "permanent" `sample.uuid_sample_lims` - LIMS-specific sample uuid' AFTER id_sample_tmp;

ALTER TABLE tol_sample_bioproject CHANGE COLUMN `file` `file` varchar(500)
COMMENT 'Path to the source file';

ALTER TABLE tol_sample_bioproject CHANGE COLUMN filename filename varchar(255)
COMMENT 'Name which should be given to the submitted file' AFTER file;

ALTER TABLE tol_sample_bioproject ADD COLUMN platform varchar(40)
COMMENT 'ENA PLATFORM metadata field. Not required if INSTRUMENT is provided' AFTER filename;

ALTER TABLE tol_sample_bioproject ADD COLUMN instrument varchar(40)
COMMENT 'ENA INSTRUMENT metadata field' AFTER platform;

ALTER TABLE tol_sample_bioproject ADD COLUMN library_name varchar(40)
COMMENT 'ENA LIBRARY_NAME metadata field' AFTER instrument;

ALTER TABLE tol_sample_bioproject ADD COLUMN library_source varchar(40)
COMMENT 'ENA LIBRARY_SOURCE metadata field' AFTER library_name;

ALTER TABLE tol_sample_bioproject ADD COLUMN library_selection varchar(40)
COMMENT 'ENA LIBRARY_SELECTION metadata field' AFTER library_source;

ALTER TABLE tol_sample_bioproject ADD COLUMN library_strategy varchar(40)
COMMENT 'ENA LIBRARY_STRATEGY metadata field' AFTER library_selection;

ALTER TABLE tol_sample_bioproject CHANGE COLUMN library_type library_type varchar(40)
COMMENT 'Temporary home for ENA LIBRARY_CONSTRUCTION_PROTOCOL metadata field during migration to library_construction_protocol';

ALTER TABLE tol_sample_bioproject ADD COLUMN library_construction_protocol varchar(40)
COMMENT 'ENA LIBRARY_CONSTRUCTION_PROTOCOL metadata field' AFTER library_type;

ALTER TABLE tol_sample_bioproject ADD COLUMN design_description varchar(500)
COMMENT 'ENA DESCRIPTION metadata field. Free text description of the library' AFTER library_construction_protocol;

ALTER TABLE tol_sample_bioproject CHANGE COLUMN tolid tolid varchar(40)
COMMENT 'Tree of Life ID, see: https://id.tol.sanger.ac.uk';

ALTER TABLE tol_sample_bioproject CHANGE COLUMN biosample_accession biosample_accession varchar(255)
COMMENT 'ENA biosample accession for the sample';

ALTER TABLE tol_sample_bioproject CHANGE COLUMN bioproject_accession bioproject_accession varchar(255)
COMMENT 'ENA project data accession for the species';

ALTER TABLE tol_sample_bioproject CHANGE COLUMN date_added date_added timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
COMMENT 'When row was created';

ALTER TABLE tol_sample_bioproject CHANGE COLUMN date_updated date_updated timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
COMMENT 'When row was last updated';


-- Indexes:

ALTER TABLE tol_sample_bioproject ADD UNIQUE (data_id);

ALTER TABLE tol_sample_bioproject ADD CONSTRAINT fk_tsb_to_sample_uuid
FOREIGN KEY fk_tsb_to_sample_uuid (uuid_sample_lims)
REFERENCES sample (uuid_sample_lims) ON DELETE SET NULL ON UPDATE RESTRICT;


-- Update data:

UPDATE tol_sample_bioproject
SET library_construction_protocol = library_type
WHERE library_construction_protocol IS NULL;

UPDATE tol_sample_bioproject AS tsb
JOIN sample USING (id_sample_tmp)
SET tsb.uuid_sample_lims = sample.uuid_sample_lims;


SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
