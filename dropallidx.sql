CREATE OR REPLACE FUNCTION drop_all_indexes() RETURNS INTEGER AS $$
DECLARE
  i RECORD;
BEGIN
    RAISE INFO 'start drop function';
  FOR i IN
    (SELECT relname FROM pg_class
       -- exclude all pkey, exclude system catalog which starts with 'pg_'
      WHERE relkind = 'i' AND OID not in (select indexrelid from pg_index where indisunique = 't') and relname not like 'pg_%')
  LOOP
    RAISE INFO 'DROPING INDEX: %', i.relname;
    EXECUTE 'DROP INDEX "' || i.relname || '";';
  END LOOP;
RETURN 1;
EXCEPTION
    WHEN plpgsql_error THEN
    RAISE NOTICE 'plpgsql error';
END;
$$ LANGUAGE plpgsql;
