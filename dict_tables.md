


#### `accession_type_dict`

accession_type_id       | regexp                                        | URL
------------------------|-----------------------------------------------|----
GenBank Genome Assembly | `^GCA_\d+\.\d+$`                              | https://www.ncbi.nlm.nih.gov/datasets/genome/{}/
Bio Sample              | `^SAMEA\d+$`                                  | https://www.ebi.ac.uk/biosamples/samples/{}
Bio Project             | `^PRJ[A-Z]{2}\d+$`                            | https://www.ebi.ac.uk/ena/browser/view/{}
ENA Run                 | `^ERR\d+$`                                    | https://www.ebi.ac.uk/ena/browser/view/{}
ENA Experiment          | `^ERX\d+$`                                    | https://www.ebi.ac.uk/ena/browser/view/{}
ToL Specimen ID         | `^[a-z]{1,2}[A-Z][a-z]{2}[A-Z][a-z]{2,3}\d+$` |


#### `sex`

sex.id,description
M,Male
F,Female
M?,Male (uncertain)
F?,Female (uncertain)
H,Hermaphrodite
H/M,"Hermaphrodite, monoecious"
NA,Not applicable
U,Unknown

#### `specimen_status_type`

#### `dataset_status_type`

#### `assembly_status_type`

#### `qc_dict`

qc_state |
---------|
pass     |
fail     |

#### `centre`

#### `platform`

#### `library_type`

#### `review_dict`

#### `software_version`

