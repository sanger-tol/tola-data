-- Query to show pattern of PacBio run metrics fileds which are not filled

SELECT (regexp_match(run_id, '^m[^_]+_(\d{4})'))[1] AS month
  , CONCAT(
    CASE WHEN movie_minutes IS NULL THEN '0' ELSE '1' END
    , CASE WHEN binding_kit IS NULL THEN '0' ELSE '1' END
    , CASE WHEN sequencing_kit IS NULL THEN '0' ELSE '1' END
    , CASE WHEN include_kinetics IS NULL THEN '0' ELSE '1' END
    , CASE WHEN loading_conc IS NULL THEN '0' ELSE '1' END
    , CASE WHEN control_num_reads IS NULL THEN '0' ELSE '1' END
    , CASE WHEN control_read_length_mean IS NULL THEN '0' ELSE '1' END
    , CASE WHEN polymerase_read_bases IS NULL THEN '0' ELSE '1' END
    , CASE WHEN polymerase_num_reads IS NULL THEN '0' ELSE '1' END
    , CASE WHEN polymerase_read_length_mean IS NULL THEN '0' ELSE '1' END
    , CASE WHEN polymerase_read_length_n50 IS NULL THEN '0' ELSE '1' END
    , CASE WHEN insert_length_mean IS NULL THEN '0' ELSE '1' END
    , CASE WHEN insert_length_n50 IS NULL THEN '0' ELSE '1' END
    , CASE WHEN unique_molecular_bases IS NULL THEN '0' ELSE '1' END
    , CASE WHEN p0_num IS NULL THEN '0' ELSE '1' END
    , CASE WHEN p1_num IS NULL THEN '0' ELSE '1' END
    , CASE WHEN p2_num IS NULL THEN '0' ELSE '1' END
    , CASE WHEN hifi_read_bases IS NULL THEN '0' ELSE '1' END
    , CASE WHEN hifi_num_reads IS NULL THEN '0' ELSE '1' END
    , CASE WHEN hifi_low_quality_num_reads IS NULL THEN '0' ELSE '1' END
  ) AS fields_filled
  , count(*) AS n
FROM pacbio_run_metrics
GROUP BY month, fields_filled
ORDER BY month DESC, fields_filled DESC;
