SELECT sample.friendly_name name
  , DATE_FORMAT(MIN(events.created_at)
      , '%Y-%m-%dT%H:%i:%s') order_date
  , mlwh_sample.public_name
  , mlwh_sample.common_name
  , mlwh_sample.supplier_name
  , mlwh_sample.accession_number
  , mlwh_sample.donor_id
  , mlwh_sample.taxon_id
  , mlwh_sample.description
FROM mlwh_events.events
JOIN mlwh_events.event_types
  ON event_types.id = events.event_type_id
JOIN mlwh_events.roles AS sample_roles
  ON events.id = sample_roles.event_id
  AND sample_roles.role_type_id = 6
JOIN mlwh_events.subjects AS sample
  ON sample_roles.subject_id = sample.id
JOIN mlwh_events.roles AS study_roles
  ON events.id = study_roles.event_id
  AND study_roles.role_type_id = 2
JOIN mlwh_events.subjects AS study
  ON study_roles.subject_id = study.id
LEFT JOIN mlwarehouse.sample mlwh_sample
  ON sample.friendly_name = mlwh_sample.name
  AND mlwh_sample.id_lims = 'SQSCP'
WHERE event_types.key = 'order_made'
  AND study.uuid = UNHEX(REPLACE('cf04ea86-ac82-11e9-8998-68b599768938', '-', ''))
GROUP BY sample.friendly_name
ORDER BY order_date
  , name


  -- field                            type              null  key  default  extra
  -- -------------------------------  ----------------  ----  ---  -------  --------------
  -- id_sample_tmp                    int(10) unsigned  NO    PRI           auto_increment
  -- id_lims                          varchar(10)       NO
  -- uuid_sample_lims                 varchar(36)       YES   UNI
  -- id_sample_lims                   varchar(20)       NO    MUL
  -- last_updated                     datetime          NO
  -- recorded_at                      datetime          NO
  -- deleted_at                       datetime          YES
  -- created                          datetime          YES
  -- name                             varchar(255)      YES   MUL
  -- reference_genome                 varchar(255)      YES
  -- organism                         varchar(255)      YES
  -- accession_number                 varchar(50)       YES   MUL
  -- common_name                      varchar(255)      YES
  -- description                      text              YES
  -- taxon_id                         int(6) unsigned   YES
  -- father                           varchar(255)      YES
  -- mother                           varchar(255)      YES
  -- replicate                        varchar(255)      YES
  -- ethnicity                        varchar(255)      YES
  -- gender                           varchar(20)       YES
  -- cohort                           varchar(255)      YES
  -- country_of_origin                varchar(255)      YES
  -- geographical_region              varchar(255)      YES
  -- sanger_sample_id                 varchar(255)      YES   MUL
  -- control                          tinyint(1)        YES
  -- supplier_name                    varchar(255)      YES   MUL
  -- public_name                      varchar(255)      YES
  -- sample_visibility                varchar(255)      YES
  -- strain                           varchar(255)      YES
  -- consent_withdrawn                tinyint(1)        NO               0
  -- donor_id                         varchar(255)      YES
  -- phenotype                        varchar(255)      YES
  -- developmental_stage              varchar(255)      YES
  -- control_type                     varchar(255)      YES
  -- sibling                          varchar(255)      YES
  -- is_resubmitted                   tinyint(1)        YES
  -- date_of_sample_collection        varchar(255)      YES
  -- date_of_sample_extraction        varchar(255)      YES
  -- extraction_method                varchar(255)      YES
  -- purified                         varchar(255)      YES
  -- purification_method              varchar(255)      YES
  -- customer_measured_concentration  varchar(255)      YES
  -- concentration_determined_by      varchar(255)      YES
  -- sample_type                      varchar(255)      YES
  -- storage_conditions               varchar(255)      YES
  -- genotype                         varchar(255)      YES
  -- age                              varchar(255)      YES
  -- cell_type                        varchar(255)      YES
  -- disease_state                    varchar(255)      YES
  -- compound                         varchar(255)      YES
  -- dose                             varchar(255)      YES
  -- immunoprecipitate                varchar(255)      YES
  -- growth_condition                 varchar(255)      YES
  -- organism_part                    varchar(255)      YES
  -- time_point                       varchar(255)      YES
  -- disease                          varchar(255)      YES
  -- subject                          varchar(255)      YES
  -- treatment                        varchar(255)      YES
  -- date_of_consent_withdrawn        datetime          YES
  -- marked_as_consent_withdrawn_by   varchar(255)      YES
  -- customer_measured_volume         varchar(255)      YES
  -- gc_content                       varchar(255)      YES
  -- dna_source                       varchar(255)      YES
