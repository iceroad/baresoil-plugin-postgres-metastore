/*
 * Insert if not exists, returning metadata or custom error message
 */
CREATE OR REPLACE FUNCTION kvd_insert(
  in_table text,
  in_id bigint,
  in_key text,
  in_encoding text,
  in_value bytea,
  in_version bytea,
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
BEGIN
  RETURN QUERY INSERT INTO kvd_metastore (
    table_name,
    table_id,
    table_key,
    value,
    value_encoding,
    value_version
  )
  VALUES (
    in_table,
    in_id,
    in_key,
    in_value,
    in_encoding,
    in_version
  )
  RETURNING
    kvd_metastore.table_name::text,
    kvd_metastore.table_id::bigint,
    kvd_metastore.table_key::text,
    NULL::bytea AS value,
    TRUE::boolean AS value_exists,
    kvd_metastore.value_encoding::text,
    kvd_metastore.value_version::bytea,
    kvd_metastore.value_modified::bigint;
  EXCEPTION
    WHEN unique_violation THEN
      RAISE EXCEPTION '%', in_errorMessage
      USING ERRCODE = 'unique_violation';
END $$;

