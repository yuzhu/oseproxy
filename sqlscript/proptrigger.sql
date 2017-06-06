CREATE OR REPLACE FUNCTION trigger_async_log() RETURNS TRIGGER AS $body$
DECLARE
    hist_row migration.hist_log;
    h_old hstore;
    h_new hstore;
BEGIN
    IF TG_WHEN <> 'AFTER' THEN
        RAISE EXCEPTION 'trigger_async_log() may only run as an AFTER trigger';
    END IF;

    hist_row = ROW(
    nextval('migration.hist_log_op_id_seq'), -- event_id
    TG_TABLE_SCHEMA::text,                        -- schema_name
    TG_TABLE_NAME::text,                          -- table_name
    substring(TG_OP,1,1),                         -- action
    NULL, NULL,                                   -- old, new
    TG_ARGV[0]::integer                           -- iteration
    );

    IF (TG_OP = 'UPDATE' AND TG_LEVEL = 'ROW') THEN
        hist_row.old_data = hstore(OLD.*);
        hist_row.new_data =  hstore(NEW.*);
    ELSIF (TG_OP = 'DELETE' AND TG_LEVEL = 'ROW') THEN
        hist_row.old_data = hstore(OLD.*);
    ELSIF (TG_OP = 'INSERT' AND TG_LEVEL = 'ROW') THEN
        hist_row.new_data =  hstore(NEW.*);
    ELSE
        RAISE EXCEPTION 'Trigger func added as trigger for unhandled case: %, %',TG_OP, TG_LEVEL;
        RETURN NULL;
    END IF;

    INSERT INTO migration.hist_log VALUES (hist_row.*);
    RETURN NULL;
END;
$body$
language 'plpgsql';




CREATE OR REPLACE FUNCTION play_log(src_schema_name TEXT, src_table_name TEXT, action TEXT, old_data hstore, new_data hstore, dest_schema_name TEXT, dest_table_name TEXT) RETURNS INT AS $body$
DECLARE
    m text[];
    rec text[][];
    collist text;
    query  text;

BEGIN

    IF (action = 'I') THEN
       EXECUTE  format('INSERT INTO %s.%s (select * from populate_record(null::%s.%s, ''%s''))', dest_schema_name, dest_table_name, dest_schema_name, dest_table_name, new_data);

       --'SELECT * FROM populate_record(null::'' || dest_schema_name || ''.'' || dest_table_name ||'',''|| new_data || '')'';
    ELSIF (action = 'D') THEN
       rec := %# old_data;
       query:= '1=1';
       FOREACH m SLICE 1 IN ARRAY rec
       LOOP
           query := query || format(' and %s = ''%s''', m[1], m[2]);
       END LOOP;
       --RAISE NOTICE 'where clause %', query;
       EXECUTE format('DELETE FROM %s.%s  WHERE %s', dest_schema_name, dest_table_name, query);
    ELSIF (action = 'U') THEN
       rec := %# old_data;
       query:= '1=1';
       collist:='';
       FOREACH m SLICE 1 IN ARRAY rec
       LOOP
           --collist := collist || format ('%s,', m[1]);
           
           query := query || format(' and %s = ''%s''', m[1], m[2]);
       END LOOP;
       -- collist := left(collist, -1);

       EXECUTE 'select string_agg(column_name, '','')  from information_schema.columns where table_name= $1 and table_schema= $2;' INTO collist using dest_table_name , dest_schema_name;
       --RAISE NOTICE 'collist %', collist;

       EXECUTE format('UPDATE %s.%s SET (%s) = (select * from populate_record(null::%s.%s, ''%s'')) WHERE %s', dest_schema_name, dest_table_name, collist, dest_schema_name, dest_table_name, new_data, query);

    ELSE
        RAISE EXCEPTION 'Trigger func added as trigger for unhandled case: %, %',TG_OP, TG_LEVEL;
        RETURN NULL;
    END IF;

    RETURN NULL;
END;
$body$
language 'plpgsql';

