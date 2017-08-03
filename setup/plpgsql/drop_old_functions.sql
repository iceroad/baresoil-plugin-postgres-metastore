/*
 * Drop all forms of all kvd_ functions
 */
DO $$
DECLARE qstr varchar;
BEGIN
  SELECT string_agg(format('DROP FUNCTION %s(%s);'
               , oid::regproc
               , pg_get_function_identity_arguments(oid)), '')
  FROM  pg_proc
  WHERE proname ~ '^kvd_'
  INTO qStr;
  IF qStr IS NOT NULL THEN EXECUTE qStr; END IF;
END $$;
