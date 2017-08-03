/*
 * Conditional update, returning metadata or custom error message
 */
CREATE OR REPLACE FUNCTION kvd_update_by_id(
  in_table text,
  in_id bigint,
  in_ifVersion bytea,
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
#variable_conflict use_column
BEGIN
  RETURN QUERY
    UPDATE kvd_metastore
    SET
      value = in_value,
      value_encoding = in_encoding,
      value_version = in_version,
      value_modified = (1000 * EXTRACT(EPOCH FROM now()))::bigint
    WHERE
      table_name = in_table
      AND table_id = in_id
      AND value_version = in_ifVersion
    RETURNING
      kvd_metastore.table_name::text,
      kvd_metastore.table_id::bigint,
      kvd_metastore.table_key::text,
      NULL::bytea AS value,
      TRUE::boolean AS value_exists,
      kvd_metastore.value_encoding::text,
      kvd_metastore.value_version::bytea,
      kvd_metastore.value_modified::bigint
    ;
  IF NOT FOUND THEN
    RAISE EXCEPTION '%', in_errorMessage
    USING ERRCODE = 'check_violation';
  END IF;
END $$;


CREATE OR REPLACE FUNCTION kvd_update_by_key(
  in_table text,
  in_key text,
  in_ifVersion bytea,
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
#variable_conflict use_column
BEGIN
  RETURN QUERY
    UPDATE kvd_metastore
    SET
      value = in_value,
      value_encoding = in_encoding,
      value_version = in_version,
      value_modified = (1000 * EXTRACT(EPOCH FROM now()))::bigint
    WHERE
      table_name = in_table
      AND table_key = in_key
      AND value_version = in_ifVersion
    RETURNING
      kvd_metastore.table_name::text,
      kvd_metastore.table_id::bigint,
      kvd_metastore.table_key::text,
      NULL::bytea AS value,
      TRUE::boolean AS value_exists,
      kvd_metastore.value_encoding::text,
      kvd_metastore.value_version::bytea,
      kvd_metastore.value_modified::bigint
    ;
  IF NOT FOUND THEN
    RAISE EXCEPTION '%', in_errorMessage
    USING ERRCODE = 'check_violation';
  END IF;
END $$;
