-- Generic MERGE template

MERGE INTO {{silver_table}} AS tgt
USING {{staging_view}} AS src
ON tgt.{{merge_key}} = src.{{merge_key}}
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
