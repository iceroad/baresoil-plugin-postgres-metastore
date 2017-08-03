/*
 * Conditional delete.
 */
CREATE OR REPLACE FUNCTION kvd_delete_by_id(
  in_table text,
  in_id bigint,
  in_ifVersion bytea,
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
    DELETE FROM kvd_metastore
    WHERE
      table_name = in_table
      AND table_id = in_id
      AND (
        value_version = in_ifVersion
        OR in_ifVersion IS NULL)
    RETURNING
      kvd_metastore.table_name::text,
      kvd_metastore.table_id::bigint,
      kvd_metastore.table_key::text,
      NULL::bytea AS value,
      FALSE::boolean AS value_exists,
      NULL::text AS value_encoding,
      NULL::bytea AS value_version,
      NULL::bigint AS value_modified;
    IF NOT FOUND THEN
      RAISE EXCEPTION '%', in_errorMessage
      USING ERRCODE = 'check_violation';
    END IF;
END $$;


CREATE OR REPLACE FUNCTION kvd_delete_by_key(
  in_table text,
  in_key text,
  in_ifVersion bytea,
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
    DELETE FROM kvd_metastore
    WHERE
      table_name = in_table
      AND table_key = in_key
      AND (
        value_version = in_ifVersion
        OR in_ifVersion IS NULL)
    RETURNING
      kvd_metastore.table_name::text,
      kvd_metastore.table_id::bigint,
      kvd_metastore.table_key::text,
      NULL::bytea AS value,
      FALSE::boolean AS value_exists,
      NULL::text AS value_encoding,
      NULL::bytea AS value_version,
      NULL::bigint AS value_modified;
    IF NOT FOUND THEN
      RAISE EXCEPTION '%', in_errorMessage
      USING ERRCODE = 'check_violation';
    END IF;
END $$;
