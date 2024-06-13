SELECT ds.dataset_id
  , array_agg(data.name)  -- Might become an array in the JSON from the API?
FROM dataset
JOIN dataset_element USING(dataset_id)
JOIN data USING(data_id)
WHERE data.name IN ('48728_7-8#1', 'm84047_240223_152838_s3#2013')
GROUP BY dataset.dataset_id
HAVING COUNT(*) = 2;  /* Required to exclude datasets which include
                         these two data rows and more */
