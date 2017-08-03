/*
 * Gets a metadata row by primary id if it exists.
 */
CREATE OR REPLACE FUNCTION kvd_get_by_id(
  in_table text,
  in_id bigint,
  in_errorMessage text)
RETURNS TABLE (
  table_name text,
  table_id bigint,
  table_key text,
  value bytea,
  value_exists boolean,
  value_encoding text,
  value_version bytea,
  value_modified bigint
)
LANGUAGE plpgsql
AS $$
#variable_conflict use_column
BEGIN
  RETURN QUERY
    SELECT
      kvd_metastore.table_name::text AS table_name,
      kvd_metastore.table_id::bigint AS table_id,
      kvd_metastore.table_key::text AS table_key,
      kvd_metastore.value::bytea AS value,
      TRUE::boolean AS value_exists,
      kvd_metastore.value_encoding::text AS value_encoding,
      kvd_metastore.value_version::bytea AS value_version,
      kvd_metastore.value_modified::bigint AS value_modified
    FROM
      kvd_metastore
    WHERE
      kvd_metastore.table_name = in_table
      AND kvd_metastore.table_id = in_id
    ;
  IF NOT FOUND THEN
    RETURN QUERY SELECT
      in_table::text AS table_name,
      in_id::bigint AS table_id,
      NULL::text AS table_key,
      NULL::bytea AS value,
      FALSE::boolean AS value_exists,
      NULL::text AS value_encoding,
      NULL::bytea AS value_version,
      NULL::bigint AS value_modified
    FROM
      kvd_metastore
    LIMIT 1;
  END IF;
END $$;


/*
 * Gets a metadata row by unique key if it exists.
 */
CREATE OR REPLACE FUNCTION kvd_get_by_key(
  in_table text,
  in_key text,
  in_errorMessage text)
RETURNS TABLE (
  table_name text,
  table_id bigint,
  table_key text,
  value bytea,
  value_exists boolean,
  value_encoding text,
  value_version bytea,
  value_modified bigint
)
LANGUAGE plpgsql
AS $$
#variable_conflict use_column
BEGIN
  RETURN QUERY
    SELECT
      kvd_metastore.table_name::text AS table_name,
      kvd_metastore.table_id::bigint AS table_id,
      kvd_metastore.table_key::text AS table_key,
      kvd_metastore.value::bytea AS value,
      TRUE::boolean AS value_exists,
      kvd_metastore.value_encoding::text AS value_encoding,
      kvd_metastore.value_version::bytea AS value_version,
      kvd_metastore.value_modified::bigint AS value_modified
    FROM
      kvd_metastore
    WHERE
      kvd_metastore.table_name = in_table
      AND kvd_metastore.table_key = in_key
    ;
  IF NOT FOUND THEN
    RETURN QUERY SELECT
      in_table::text AS table_name,
      NULL::bigint AS table_id,
      in_key::text AS table_key,
      NULL::bytea AS value,
      FALSE::boolean AS value_exists,
      NULL::text AS value_encoding,
      NULL::bytea AS value_version,
      NULL::bigint AS value_modified
    FROM
      kvd_metastore
    LIMIT 1;
  END IF;
END $$;
