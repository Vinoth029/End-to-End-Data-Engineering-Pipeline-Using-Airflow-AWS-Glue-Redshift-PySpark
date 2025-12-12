COPY staging_mydataset
FROM 's3://my-processed-bucket/processed/mydataset/{{ ds_nodash }}/'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftCopyRole'
FORMAT AS PARQUET;
