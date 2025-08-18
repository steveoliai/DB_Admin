--in the procedures below, change the dblink authentication to yours
--connect to main postgresDB (or whatever DB you want to manage things from)

--needed extention for separate DB per tenant
CREATE EXTENSION IF NOT EXISTS dblink;

CREATE SCHEMA admmgt
    AUTHORIZATION postgres;


--table to store db (or schema) name of vendor/tenant
create table admmgt.vendor_db_settings (
    id bigserial, 
    dbname varchar(128),
    createdon timestamp default current_timestamp,
    modifiedon timestamp default current_timestamp,
    deleted boolean default false,
    deletedon timestamp,
    scriptversion int,
    updateflag boolean default true,
    istemplate boolean default false,
    status int default 0, --0 = designing, 1 = pending, 2 = created
CONSTRAINT vendor_db_settings_pkey PRIMARY KEY (id));

create unique index vendor_db_settings_dbname on admmgt.vendor_db_settings(upper(dbname), deleted);

--this table is used to capture stored procedure changes made on template DB in order to replay past migrations
--for database set to NOT update with others

create table  admmgt.script_proc_log(id bigserial, scriptid integer, createdon timestamp default current_timestamp, ispremigration boolean, commstat TEXT);

ALTER TABLE admmgt.script_proc_log ADD CONSTRAINT script_proc_log_pkey PRIMARY KEY(id);

create index script_proc_log_scriptid on admmgt.script_proc_log(scriptid);

--this table is used to capture view changes made on template DB in order to replay past migrations


create table  admmgt.script_view_log(id bigserial, scriptid integer, createdon timestamp default current_timestamp, ispremigration boolean, commstat TEXT);

ALTER TABLE admmgt.script_view_log ADD CONSTRAINT script_view_log_pkey PRIMARY KEY(id);

create index script_view_log_scriptid on admmgt.script_view_log(scriptid);


create table admmgt.scripts(
    id INT,
    description varchar(1000),
    status integer default 0, --0 = designing, 1 = ready to apply, 2 = applied, 3 = failed to apply
    created_date timestamp default CURRENT_TIMESTAMP
);

ALTER TABLE admmgt.scripts ADD CONSTRAINT scripts_pkey PRIMARY KEY(id);


create table admmgt.script_tables(
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    scriptid int,
    schemaname varchar(128),
    tablename varchar(128),
    status integer default 0, --0 = designing, 1 = ready to create, 2 = created, 3 = failed to create
    haspart integer default 0,
    lifecycle jsonb, --store JSON for definition
    tblspace varchar(128),
    created_date timestamp default CURRENT_TIMESTAMP
);


ALTER TABLE admmgt.script_tables ADD CONSTRAINT script_tables_pkey PRIMARY KEY(id);

--add RI

ALTER TABLE admmgt.script_tables ADD CONSTRAINT fk_script_tables_scripts FOREIGN KEY (scriptid) REFERENCES admmgt.scripts(id);

create index script_tables_scriptid on admmgt.script_tables(scriptid);

create table admmgt.script_table_columns(
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    tableid integer,
    scriptid integer,
    columnname varchar(128),
    datatype varchar(128),
    nullable  integer,
    defaultval varchar(128),
    isprimarykey integer default 0,
    isidentity integer default 0,
    status integer default 0,
    created_date timestamp default CURRENT_TIMESTAMP
);

ALTER TABLE admmgt.script_table_columns ADD CONSTRAINT script_table_columns_pkey PRIMARY KEY(id);


--add RI

ALTER TABLE admmgt.script_table_columns ADD CONSTRAINT fk_script_tables_columns FOREIGN KEY (tableid) REFERENCES admmgt.script_tables(id);

create index script_table_columns_tableid on admmgt.script_table_columns(tableid);

create table admmgt.script_table_fkeys(
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    scriptid integer,
    schemaname varchar(128),
    childtablename varchar(128),
	childcolumnname varchar(128),
    parenttablename  varchar(128),
    parentcolumnname varchar(128),
    keyorder integer default 1,
    delrule varchar(32),
	novalidate integer default 0,
    status integer default 0,
    created_date timestamp default CURRENT_TIMESTAMP
);


ALTER TABLE admmgt.script_table_fkeys ADD CONSTRAINT script_table_fkeys_pkey PRIMARY KEY(id);

--add RI

ALTER TABLE admmgt.script_table_fkeys ADD CONSTRAINT fk_script_table_fkeys_scripts FOREIGN KEY (scriptid) REFERENCES admmgt.scripts(id);


create table admmgt.script_table_partitions(
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    tableid integer,
    partcolumnname varchar(128),
    parttype varchar(10),
    partvalue varchar(20),
    parttbs varchar(128),
    subpartcolumnname varchar(128),
    subparttype varchar(10),
    subpartvalue varchar(20),
    subparttbs varchar(128),
    created_date timestamp default CURRENT_TIMESTAMP
);

ALTER TABLE admmgt.script_table_partitions ADD CONSTRAINT script_table_partitions_pkey PRIMARY KEY(id);

--ADD RI

ALTER TABLE admmgt.script_table_partitions ADD CONSTRAINT fk_script_tables_partitions FOREIGN KEY (tableid) REFERENCES admmgt.script_tables(id);

create index script_table_partitions_tableid on admmgt.script_table_partitions(tableid);


--view to get column list for PK (partitioning keys MUST be part of PK in PostgreSQL)

create or replace view admmgt.script_pg_pklist as	
select columnname, tableid from admmgt.script_table_columns where isprimarykey = 1
union distinct
select partcolumnname as columnname, tableid  from admmgt.script_table_partitions
union distinct
select subpartcolumnname as columnname, tableid from admmgt.script_table_partitions where subpartcolumnname is not null;


create table admmgt.script_procs(
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    scriptid int,
    proccallstmt varchar(128),
    status integer default 0, --0 = designing, 1 = ready to create, 2 = created, 3 = failed to create
    created_date timestamp default CURRENT_TIMESTAMP
);


ALTER TABLE admmgt.script_procs ADD CONSTRAINT script_procs_pkey PRIMARY KEY(id);

--add RI

ALTER TABLE admmgt.script_procs ADD CONSTRAINT fk_script_procs_scripts FOREIGN KEY (scriptid) REFERENCES admmgt.scripts(id);


--this table is used to store dynamic statements to log script execution

create table  admmgt.script_log(id bigserial, scriptid integer, dbname varchar(128), createdon timestamp default current_timestamp, commstat TEXT);

ALTER TABLE admmgt.script_log ADD CONSTRAINT script_log_pkey PRIMARY KEY(id);

create index script_log_scriptid on admmgt.script_log(scriptid, dbname);


--when called, this will create databases as inserted into vendor_db_settings
CREATE OR REPLACE PROCEDURE admmgt.create_database()
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    t_id bigint;
    t_dbname varchar;
    t_cmd TEXT;
    t_scriptversion int;
BEGIN
    select coalesce(scriptversion, 0) into t_scriptversion from admmgt.vendor_db_settings where istemplate = true;

    FOR t_id, t_dbname in (select id, lower(dbname) from admmgt.vendor_db_settings where status = 1)
    LOOP
        -- Dynamic SQL to create the database
        t_cmd := 'CREATE DATABASE ' || quote_ident(lower(t_dbname))||' TEMPLATE unitemplate';

        PERFORM dblink_exec('dbname= postgres user=postgres password=your-password port = 5432', t_cmd);

        update admmgt.vendor_db_settings set scriptversion = t_scriptversion, status = 2  where id = t_id;
    END LOOP;    

END;
$BODY$;

--when called, this will create schemas in the current DB.  The schema name is from vendor_db_settings.dbname

CREATE OR REPLACE PROCEDURE admmgt.create_schema()
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    t_id bigint;
    t_dbname varchar;
    t_cmd TEXT;
    t_scriptversion int;
BEGIN
    select coalesce(scriptversion, 0) into t_scriptversion from admmgt.vendor_db_settings where istemplate = true;

    FOR t_id, t_dbname in (select id, lower(dbname) from admmgt.vendor_db_settings where status = 1)
    LOOP
        -- Dynamic SQL to create the database
        t_cmd := 'CREATE SCHEMA ' || quote_ident(lower(t_dbname))||' AUTHORIZATION postgres';
        
        EXECUTE t_cmd;
        
        --run replay procedure to get new schema to same level


        update admmgt.vendor_db_settings set scriptversion = t_scriptversion, status = 2  where id = t_id;
    END LOOP;    

END;
$BODY$;

--when called, this procedure will loop through procedures in the template DB and apply them to the tenants
--it will be called automatically before and after a script is applied.

CREATE OR REPLACE PROCEDURE  admmgt.refesh_stored_procedures(t_separatedb IN integer, t_scriptid IN integer)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    t_cmd TEXT;
    t_schema varchar;
    t_id bigint;
    t_dbname varchar;
    t_template_proc_record RECORD;
    t_template_conn_str TEXT;
    t_remote_conn_str TEXT;
    t_maxscriptid integer;
    t_ispremigration boolean;

BEGIN
	
    select lower(dbname) into t_dbname  from admmgt.vendor_db_settings where status = 2 and istemplate  = true and updateflag = true;
    t_template_conn_str := 'dbname ='||t_dbname||' user=postgres password=your-password port = 5432';

    t_schema := t_dbname;

    select coalesce(max(id),0) into t_maxscriptid from admmgt.scripts where status = 1;

    --we need to know is migration procedures were applied before the other schema changes
    --this way we can replicate what happened on a DB that doesn't upgrade with the template

    if t_maxscriptid = t_scriptid then --this is a pre migration run
        t_ispremigration := true;
    else
        t_ispremigration := false;
    end if;

    --get stored procs in mgttest schema from template db excluding trigger functions
    t_cmd := 'SELECT
                pg_catalog.pg_get_functiondef(p.oid) AS body
                FROM
                    pg_catalog.pg_proc p
                JOIN
                    pg_catalog.pg_namespace n ON n.oid = p.pronamespace

                WHERE
                    n.nspname LIKE (''mgttest%'')
                    AND p.prorettype <> ''pg_catalog.trigger''::pg_catalog.regtype 
                ORDER BY
                    p.oid';

    --loop to backup current proc definitions
    if t_separatedb = 1 then --if separate db per tenant
        FOR t_template_proc_record IN
            SELECT body
            FROM dblink(t_template_conn_str, t_cmd)
            AS remote_procs(body TEXT)
        LOOP
            insert into admmgt.script_proc_log(scriptid, ispremigration, commstat)
                values (t_scriptid, t_ispremigration, t_template_proc_record.body);

        END LOOP;
    else --separate schema per tenant
        FOR t_template_proc_record IN
            SELECT
                pg_catalog.pg_get_functiondef(p.oid) AS body
                FROM
                    pg_catalog.pg_proc p
                JOIN
                    pg_catalog.pg_namespace n ON n.oid = p.pronamespace

                WHERE
                    lower(n.nspname) = t_schema
                    AND p.prorettype <> 'pg_catalog.trigger'::pg_catalog.regtype 
                ORDER BY
                    p.oid
        LOOP
            insert into admmgt.script_proc_log(scriptid, ispremigration, commstat)
                values (t_scriptid, t_ispremigration, t_template_proc_record.body);

        END LOOP;
    end if;
    --loop to apply to tenants
    FOR t_dbname in (select lower(dbname) from admmgt.vendor_db_settings where status = 2 and istemplate  = false and updateflag = true)
    LOOP

        t_remote_conn_str := 'dbname='||t_dbname||' user=postgres password=your-password port = 5432';

    if t_separatedb = 1 then --if separate db per tenant
        FOR t_template_proc_record IN
            SELECT body
            FROM dblink(t_template_conn_str, t_cmd)
            AS remote_procs(body TEXT)
        LOOP
            -- Process each procedure
            PERFORM dblink_exec(t_remote_conn_str, t_template_proc_record.body);
            
        END LOOP;
    else --separate schema per tenant
        FOR t_template_proc_record IN
            SELECT
                pg_catalog.pg_get_functiondef(p.oid) AS body
                FROM
                    pg_catalog.pg_proc p
                JOIN
                    pg_catalog.pg_namespace n ON n.oid = p.pronamespace

                WHERE
                    lower(n.nspname) = t_schema
                    AND p.prorettype <> 'pg_catalog.trigger'::pg_catalog.regtype 
                ORDER BY
                    p.oid
        LOOP
            -- Process each procedure
            EXECUTE replace(t_template_proc_record.body, t_schema||'.', t_dbname||'.' );
            
        END LOOP;

    end if;

    END LOOP;

END;
$BODY$;


--when called, this procedure will loop through views in the template DB and apply them to the tenants
CREATE OR REPLACE PROCEDURE  admmgt.refesh_views(t_separatedb IN integer, t_scriptid IN integer)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    t_cmd TEXT;
    t_schema varchar;
    t_id bigint;
    t_dbname varchar;
    t_template_proc_record RECORD;
    t_template_conn_str TEXT;
    t_remote_conn_str TEXT;
    t_maxscriptid integer;
    t_ispremigration boolean;

BEGIN
	
    select lower(dbname) into t_dbname from admmgt.vendor_db_settings where status = 2 and istemplate  = true and updateflag = true;
    t_template_conn_str := 'dbname ='||t_dbname||' user=postgres password=your-password port = 5432';

    t_schema := t_dbname;

    select coalesce(max(id),0) into t_maxscriptid from admmgt.scripts where status = 1;

    --we need to know is migration procedures were applied before the other schema changes
    --this way we can replicate what happened on a DB that doesn't upgrade with the template

    if t_maxscriptid = t_scriptid then
        t_ispremigration := true;
    else
        t_ispremigration := false;
    end if;

    --get views in mgttest schema from template db excluding trigger functions
    t_cmd := 'SELECT ''create or replace view ''||n.nspname||''.''||c.relname||'' as ''||pg_get_viewdef(c.oid, true) AS body
                FROM
                    pg_class c
                JOIN
                    pg_namespace n ON n.oid = c.relnamespace
                WHERE
                    c.relkind = ''v''
                    and n.nspname = ''mgttest''
                ORDER BY
                    c.oid';

    --loop to backup current view definitions
    if t_separatedb = 1 then --if separate db per tenant
        FOR t_template_proc_record IN
            SELECT body
            FROM dblink(t_template_conn_str, t_cmd)
            AS remote_procs(body TEXT)
        LOOP
            insert into admmgt.script_view_log(scriptid, ispremigration, commstat)
                values (t_scriptid, t_ispremigration, t_template_proc_record.body);

        END LOOP;
    else
        FOR t_template_proc_record IN
            SELECT 'create or replace view '||n.nspname||'.'||c.relname||' as '||pg_get_viewdef(c.oid, true) AS body
                FROM
                    pg_class c
                JOIN
                    pg_namespace n ON n.oid = c.relnamespace
                WHERE
                    c.relkind = 'v'
                    and lower(n.nspname) = t_schema
                ORDER BY
                    c.oid
        LOOP
            insert into admmgt.script_view_log(scriptid, ispremigration, commstat)
                values (t_scriptid, t_ispremigration, t_template_proc_record.body);

        END LOOP;
    end if;
    --loop to apply to tenants
    FOR t_dbname in (select lower(dbname) from admmgt.vendor_db_settings where status = 2 and istemplate  = false and updateflag = true)
    LOOP

        t_remote_conn_str := 'dbname='||t_dbname||' user=postgres password=your-password port = 5432';

    if t_separatedb = 1 then --if separate db per tenant
        FOR t_template_proc_record IN
            SELECT body
            FROM dblink(t_template_conn_str, t_cmd)
            AS remote_procs(body TEXT)
        LOOP
            -- Process each procedure
            PERFORM dblink_exec(t_remote_conn_str, t_template_proc_record.body);
            
        END LOOP;
    else
        FOR t_template_proc_record IN
            SELECT 'create or replace view '||n.nspname||'.'||c.relname||' as '||pg_get_viewdef(c.oid, true) AS body
                FROM
                    pg_class c
                JOIN
                    pg_namespace n ON n.oid = c.relnamespace
                WHERE
                    c.relkind = 'v'
                    and lower(n.nspname) = t_schema
                ORDER BY
                    c.oid
        LOOP
            -- Process each procedure
            EXECUTE replace(t_template_proc_record.body, t_schema||'.', t_dbname||'.' );     

        END LOOP;

    end if;

    END LOOP;

END;
$BODY$;


--procedure to create table with partition scheme

CREATE OR REPLACE PROCEDURE admmgt.createTables(t_separatedb IN integer, t_scriptid IN INT, t_remote_conn_str IN TEXT)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    t_buildstat varchar(4000);
    t_commstat TEXT;
    t_partcheck varchar(128);
    t_subpartcheck varchar(128);
    t_pkcheck int;
    t_recid bigint;
    t_schemaname varchar(128);
    t_tablename varchar(128);
    t_partcolumnname varchar(128);
    t_partvalue varchar(20);
    t_parttbs varchar(128);
    t_subpartcolumnname varchar(128);
    t_subparttype varchar(10);
    t_subpartvalue varchar(20);
    t_subparttbs varchar(128);
    t_monthto varchar(2);
    t_yearto varchar(4);
    t_monthfrom varchar(2);
    t_yearfrom varchar(4);
    t_addtbs varchar(128);
    t_dbname varchar(128);
    t_templateschema varchar(128);
BEGIN
    --get the name of the DB this will be executed on for logging
    t_dbname := lower(trim(replace(substr(t_remote_conn_str, 1, position( 'user='  in t_remote_conn_str) - 1), 'dbname=', '')));
    --get template schema name
    select lower(dbname) into t_templateschema from admmgt.vendor_db_settings where istemplate = true;

    FOR t_recid IN (SELECT id FROM admmgt.script_tables where status = 1 and scriptid = t_scriptid) --only process tables where the definition is completed
    LOOP

        BEGIN  --start building the create table statement
            --initialize variables
            t_commstat := '';
            t_partcheck := null;
            t_subpartcheck := null;
            --table structure with columns
            select 'create table '||schemaname||'.'||tablename||' ('||string_agg(columnname||' '||datatype||' '||case when defaultval is not null then ' default '|| defaultval else '' end ||case when nullable = 0 and isidentity = 0 then ' not null ' else '' end|| case when isidentity = 1 then ' generated always as identity ' else '' end, ','
                order by c.id)  
                into t_buildstat
                from admmgt.script_table_columns c, admmgt.script_tables  t where t.id = c.tableid and t.status = 1 and t.id = t_recid group by schemaname, tablename;
            t_commstat := t_buildstat;

            --primary key
            select count(*) into t_pkcheck from admmgt.script_table_columns where tableid = t_recid and isprimarykey = 1;
            if t_pkcheck > 0 then -- primary key columns are defined
            select ', CONSTRAINT '||tablename||'_pkey PRIMARY KEY ('||string_agg(columnname,','order by c.tableid)||')'
                into t_buildstat
                from admmgt.script_pg_pklist c, admmgt.script_tables  t where t.id = c.tableid and t.status = 1 and t.id = t_recid group by tablename;
                t_commstat := t_commstat||t_buildstat;
            end if;
            --to close table definition
            t_commstat := t_commstat||')';   

            --need to set to empty string incase no partition defined
            t_buildstat := ''; 
            select max(distinct(upper(parttype))) into t_partcheck from admmgt.script_table_partitions where tableid = t_recid;

            select max(distinct(upper(subparttype))) into t_subpartcheck from admmgt.script_table_partitions where subparttype is not null and tableid = t_recid;

            --partition 
            if t_partcheck is not null then
                select DISTINCT 'PARTITION BY '||PARTTYPE||'('||PARTCOLUMNNAME||')' 
                    into t_buildstat
                    from admmgt.script_table_partitions p, admmgt.script_tables  t where t.id = p.tableid and t.status = 1 and t.id = t_recid;
                t_commstat := lower(t_commstat)||lower(t_buildstat);
            
                --create the table with initial partition definition
                --EXECUTE t_commstat; 
                if t_separatedb = 1 then --separate dbs
                    --log the statement
                    insert into admmgt.script_log(scriptid, dbname, commstat) values (t_scriptid,t_dbname, t_commstat);                 
                    PERFORM dblink_exec(t_remote_conn_str, t_commstat);
                else --separate schema
                    --log the statement
                    insert into admmgt.script_log(scriptid, dbname, commstat) values (t_scriptid,t_dbname, replace(t_commstat, t_templateschema||'.', t_dbname||'.'));                 
                    EXECUTE replace(t_commstat, t_templateschema||'.', t_dbname||'.');
                end if;
                t_commstat := '';
                t_buildstat := ''; 

                FOR t_schemaname, t_tablename, t_partcolumnname, t_partvalue, t_parttbs, t_subpartcolumnname, t_subparttype, t_subpartvalue, t_subparttbs in (select schemaname, tablename, partcolumnname, partvalue, parttbs, subpartcolumnname, subparttype, subpartvalue, subparttbs from admmgt.script_table_partitions p, admmgt.script_tables  t where t.id = p.tableid and t.status = 1 and tableid = t_recid order by p.id )
                LOOP

                    if t_partcheck = 'LIST' and (t_subpartcheck = 'RANGE' or t_subpartcheck is null) then
                        if t_subpartcolumnname is NULL then 
                            --no subpartition defined so create partition alone
                            t_addtbs:='';
                            if coalesce(t_parttbs,'')<> '' and t_parttbs <> 'pg_default' and t_parttbs <> 'pg_global' then --can't have partition creation specified in these
                                t_addtbs:= ' TABLESPACE '||t_parttbs;
                            else
                                t_addtbs:='';
                            end if;
                            t_commstat := 'create table '||lower(t_schemaname)||'.'||lower(t_tablename)||'_'||lower(t_partcolumnname)||'_'||lower(t_partvalue)||' partition of '||lower(t_schemaname)||'.'||lower(t_tablename)||' FOR VALUES IN('''||t_partvalue||''')'||t_addtbs;
                            --EXECUTE t_commstat; 
                            --PERFORM dblink_exec(t_remote_conn_str, t_commstat);
                            if t_separatedb = 1 then --separate dbs
                                --log the statement
                                insert into admmgt.script_log(scriptid, dbname, commstat) values (t_scriptid,t_dbname, t_commstat);                             
                                PERFORM dblink_exec(t_remote_conn_str, t_commstat);
                            else --separate schema
                                --log the statement
                                insert into admmgt.script_log(scriptid, dbname, commstat) values (t_scriptid,t_dbname, replace(t_commstat, t_templateschema||'.', t_dbname||'.'));                              
                                EXECUTE replace(t_commstat, t_templateschema||'.', t_dbname||'.');
                            end if;
                        else --define subpartition
                            t_addtbs:='';
                            if coalesce(t_parttbs,'')<> '' and t_parttbs <> 'pg_default' and t_parttbs <> 'pg_global' then --can't have partition creation specified in these
                                t_addtbs:= ' TABLESPACE '||t_parttbs;
                            else
                                t_addtbs:='';
                            end if;
                            t_commstat := 'create table '||lower(t_schemaname)||'.'||lower(t_tablename)||'_'||lower(t_partcolumnname)||'_'||lower(t_partvalue)||' partition of '||lower(t_schemaname)||'.'||lower(t_tablename)||' FOR VALUES IN('''||t_partvalue||''') PARTITION BY RANGE ('||t_subpartcolumnname||')'||t_addtbs;
                            --EXECUTE t_commstat; 
                            --PERFORM dblink_exec(t_remote_conn_str, t_commstat);
                            if t_separatedb = 1 then --separate dbs
                                --log the statement
                                insert into admmgt.script_log(scriptid, dbname, commstat) values (t_scriptid,t_dbname, t_commstat); 
                                PERFORM dblink_exec(t_remote_conn_str, t_commstat);
                            else --separate schema
                                --log the statement
                                insert into admmgt.script_log(scriptid, dbname, commstat) values (t_scriptid,t_dbname, replace(t_commstat, t_templateschema||'.', t_dbname||'.'));                             
                                EXECUTE replace(t_commstat, t_templateschema||'.', t_dbname||'.');
                            end if;                            
                            --create initial subpartition
                            --subpartion can only be RANGE(date) partitions so need to find initial TO and FROM values
                            if lower(t_subpartvalue) = 'month' then --partition by month
                                select (date_part('month', current_date))::varchar into t_monthfrom;
                                select case when (date_part('month', current_date)+1)::varchar = '13' then '01' else (date_part('month', current_date)+1)::varchar end into t_monthto;

                                select (date_part('year', current_date))::varchar into t_yearfrom;
                                select case when (date_part('month', current_date)+1)::varchar = '13' then (date_part('year', current_date)+ 1)::varchar else (date_part('year', current_date))::varchar end into t_yearto;

                            elsif lower(t_subpartvalue) = 'quarter' then
                                select case when (date_part('month', current_date)) < 4 then '1'
                                    when (date_part('month', current_date)) < 7 then '4'
                                    when (date_part('month', current_date)) < 10 then '7'
                                    when (date_part('month', current_date)) <= 12 then '10' end into t_monthfrom;
                                select case when t_monthfrom = '1' then '4'
                                    when t_monthfrom = '4' then '7'
                                    when t_monthfrom = '7' then '10'
                                    when t_monthfrom = '10' then '1' end into t_monthto;
                                select (date_part('year', current_date))::varchar into t_yearfrom;
                                select case when t_monthto = '1' then (date_part('year', current_date)+1)::varchar else (date_part('year', current_date))::varchar end into t_yearto;

                            elsif lower(t_subpartvalue) = 'year' then
                                t_monthfrom := '1';
                                t_monthto := '1';
                                select (date_part('year', current_date))::varchar into t_yearfrom;
                                select (date_part('year', current_date)+1)::varchar into t_yearto;

                            end if;
                            --convert month to 2 chars
                            select case when length(t_monthto) = 1 then '0'||t_monthto else t_monthto end into t_monthto;
                            select case when length(t_monthfrom) = 1 then '0'||t_monthfrom else t_monthfrom end into t_monthfrom;
                            t_addtbs:='';
                            if coalesce(t_subparttbs,'')<> '' and t_subparttbs <> 'pg_default' and t_subparttbs <> 'pg_global' then --can't have partition creation specified in these
                                t_addtbs:= ' TABLESPACE '||t_subparttbs;
                            else
                                t_addtbs:='';
                            end if;
                            t_commstat := 'CREATE TABLE '||lower(t_schemaname)||'.'||lower(t_tablename)||'_'||lower(t_partcolumnname)||'_'||lower(t_partvalue)||'_'||t_yearfrom||'_'||t_monthfrom||' partition of '||lower(t_schemaname)||'.'||lower(t_tablename)||'_'||lower(t_partcolumnname)||'_'||lower(t_partvalue)||' FOR VALUES FROM ('''||t_yearfrom||'-'||t_monthfrom||'-01'') TO ('''||t_yearto||'-'||t_monthto||'-01'')'||t_addtbs;
                            --EXECUTE t_commstat;  
                            --PERFORM dblink_exec(t_remote_conn_str, t_commstat);
                            if t_separatedb = 1 then --separate dbs
                                --log the statement
                                insert into admmgt.script_log(scriptid, dbname, commstat) values (t_scriptid,t_dbname, t_commstat);
                                PERFORM dblink_exec(t_remote_conn_str, t_commstat);
                            else --separate schema
                                --log the statement
                                insert into admmgt.script_log(scriptid, dbname, commstat) values (t_scriptid,t_dbname, replace(t_commstat, t_templateschema||'.', t_dbname||'.'));
                                EXECUTE replace(t_commstat, t_templateschema||'.', t_dbname||'.');
                            end if;                            
                        end if;

                    elsif t_partcheck = 'RANGE' and (t_subpartcheck is null) then

                        if lower(t_partvalue) = 'month' then --partition by month
                            select (date_part('month', current_date))::varchar into t_monthfrom;
                            select case when (date_part('month', current_date)+1)::varchar = '13' then '01' else (date_part('month', current_date)+1)::varchar end into t_monthto;

                            select (date_part('year', current_date))::varchar into t_yearfrom;
                            select case when (date_part('month', current_date)+1)::varchar = '13' then (date_part('year', current_date)+ 1)::varchar else (date_part('year', current_date))::varchar end into t_yearto;

                        elsif lower(t_partvalue) = 'quarter' then
                            select case when (date_part('month', current_date)) < 4 then '1'
                                when (date_part('month', current_date)) < 7 then '4'
                                when (date_part('month', current_date)) < 10 then '7'
                                when (date_part('month', current_date)) <= 12 then '10' end into t_monthfrom;
                            select case when t_monthfrom = '1' then '4'
                                when t_monthfrom = '4' then '7'
                                when t_monthfrom = '7' then '10'
                                when t_monthfrom = '10' then '1' end into t_monthto;
                            select (date_part('year', current_date))::varchar into t_yearfrom;
                            select case when t_monthto = '1' then (date_part('year', current_date)+1)::varchar else (date_part('year', current_date))::varchar end into t_yearto;

                        elsif lower(t_partvalue) = 'year' then
                            t_monthfrom := '1';
                            t_monthto := '1';
                            select (date_part('year', current_date))::varchar into t_yearfrom;
                            select (date_part('year', current_date)+1)::varchar into t_yearto;
 
                        end if;

                        --convert month to 2 chars
                        select case when length(t_monthto) = 1 then '0'||t_monthto else t_monthto end into t_monthto;
                        select case when length(t_monthfrom) = 1 then '0'||t_monthfrom else t_monthfrom end into t_monthfrom;
                    
                        t_addtbs:='';
                        if coalesce(t_parttbs,'')<> '' and t_parttbs <> 'pg_default' and t_parttbs <> 'pg_global' then --can't have partition creation specified in these
                            t_addtbs:= ' TABLESPACE '||t_parttbs;
                        else
                            t_addtbs:='';
                        end if;

                        t_commstat := 'CREATE TABLE '||lower(t_schemaname)||'.'||lower(t_tablename)||'_'||t_yearfrom||'_'||t_monthfrom||' partition of '||lower(t_schemaname)||'.'||lower(t_tablename)||' FOR VALUES FROM ('''||t_yearfrom||'-'||t_monthfrom||'-01'') TO ('''||t_yearto||'-'||t_monthto||'-01'')'||t_addtbs;
                        
                        if t_separatedb = 1 then --separate dbs
                            --log the statement
                            insert into admmgt.script_log(scriptid, dbname, commstat) values (t_scriptid,t_dbname, t_commstat); 
                            --PERFORM dblink_exec(t_remote_conn_str, t_commstat);
                            PERFORM dblink_exec(t_remote_conn_str, t_commstat);
                        else --separate schema
                            --log the statement
                            insert into admmgt.script_log(scriptid, dbname, commstat) values (t_scriptid,t_dbname, replace(t_commstat, t_templateschema||'.', t_dbname||'.')); 
                            EXECUTE replace(t_commstat, t_templateschema||'.', t_dbname||'.');
                        end if;
                    end if;

                END LOOP;
            else
                --set tablespace for table 
                select case when tblspace is null then '' else ' TABLESPACE '||tblspace||' ' end into t_buildstat from admmgt.script_tables  t where t.id = t_recid;
                t_commstat := t_commstat||t_buildstat;
                --PERFORM dblink_exec(t_remote_conn_str, t_commstat);
                if t_separatedb = 1 then --separate dbs
                    --log the statement
                    insert into admmgt.script_log(scriptid, dbname, commstat) values (t_scriptid,t_dbname, t_commstat); 
                    PERFORM dblink_exec(t_remote_conn_str, t_commstat);
                else --separate schema
                    --log the statement
                    insert into admmgt.script_log(scriptid, dbname, commstat) values (t_scriptid,t_dbname, replace(t_commstat, t_templateschema||'.', t_dbname||'.')); 
                    EXECUTE replace(t_commstat, t_templateschema||'.', t_dbname||'.');
                end if;                
            end if;

        END;
    END LOOP;
END;
$BODY$;


CREATE OR REPLACE PROCEDURE admmgt.maintainPartitions(t_separatedb IN integer, t_numdays IN INT, t_remote_conn_str IN TEXT)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    t_buildstat varchar(4000);
    t_commstat TEXT;
    t_nextdate timestamp;
    t_partitionname varchar(128);
    t_subpartitionname varchar(128);
    t_schemaname varchar(128);
    t_tablename varchar(128);
    t_partcolumnname varchar(128);
    t_partvalue varchar(20);
    t_parttbs varchar(128);
    t_subpartcolumnname varchar(128);
    t_subparttype varchar(10);
    t_subpartvalue varchar(20);
    t_subparttbs varchar(128);
    t_monthto varchar(2);
    t_yearto varchar(4);
    t_monthfrom varchar(2);
    t_yearfrom varchar(4);
    t_partcheck int;
    t_subpartcheck int;
    t_addtbs varchar(128);
    t_cmd TEXT;
    t_dbname varchar(128);
    t_templateschema varchar(128);

BEGIN
    --get the name of the DB this will be executed on for logging
    t_dbname := lower(trim(replace(substr(t_remote_conn_str, 1, position( 'user='  in t_remote_conn_str) - 1), 'dbname=', '')));
    --get template schema name
    select lower(dbname) into t_templateschema from admmgt.vendor_db_settings where istemplate = true;

    t_nextdate := current_date + t_numdays; --check if new range partition or subpartition will be needed in t_numdays days

    --partition maintenance loop
    FOR t_schemaname, t_tablename, t_partvalue, t_parttbs IN (select schemaname, tablename, partvalue, parttbs from admmgt.script_table_partitions p, admmgt.script_tables t where t.id = p.tableid and t.status = 2 and upper(parttype) = 'RANGE') --only process tables where the definition is completed
    LOOP

        if lower(t_partvalue) = 'month' then --partition by month
            select (date_part('month', t_nextdate))::varchar into t_monthfrom;
            select case when (date_part('month', t_nextdate)+1)::varchar = '13' then '01' else (date_part('month', t_nextdate)+1)::varchar end into t_monthto;

            select (date_part('year', t_nextdate))::varchar into t_yearfrom;
            select case when (date_part('month', t_nextdate)+1)::varchar = '13' then (date_part('year', t_nextdate)+ 1)::varchar else (date_part('year', t_nextdate))::varchar end into t_yearto;

        elsif lower(t_partvalue) = 'quarter' then
            select case when (date_part('month', t_nextdate)) < 4 then '1'
                when (date_part('month', t_nextdate)) < 7 then '4'
                when (date_part('month', t_nextdate)) < 10 then '7'
                when (date_part('month', t_nextdate)) <= 12 then '10' end into t_monthfrom;
            select case when t_monthfrom = '1' then '4'
                when t_monthfrom = '4' then '7'
                when t_monthfrom = '7' then '10'
                when t_monthfrom = '10' then '1' end into t_monthto;
            select (date_part('year', t_nextdate))::varchar into t_yearfrom;
            select case when t_monthto = '1' then (date_part('year', t_nextdate)+1)::varchar else (date_part('year', t_nextdate))::varchar end into t_yearto;

        elsif lower(t_partvalue) = 'year' then
            t_monthfrom := '1';
            t_monthto := '1';
            select (date_part('year', t_nextdate))::varchar into t_yearfrom;
            select (date_part('year', t_nextdate)+1)::varchar into t_yearto;

        end if;

        --convert month to 2 chars
        select case when length(t_monthto) = 1 then '0'||t_monthto else t_monthto end into t_monthto;
        select case when length(t_monthfrom) = 1 then '0'||t_monthfrom else t_monthfrom end into t_monthfrom;

        --now lets set what the name of the partition should be
        t_partitionname := lower(t_tablename)||'_'||t_yearfrom||'_'||t_monthfrom;
        t_partcheck := null;
        t_cmd = 'select count(*) as num from pg_inherits
                JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
                JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
                JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
                JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
            WHERE lower(nmsp_parent.nspname)= '''||lower(t_schemaname)||''' and lower(parent.relname) = '''||lower(t_tablename)||''' and lower(child.relname) = '''||t_partitionname||'''
            group by nmsp_parent.nspname, parent.relname, nmsp_child.nspname';
        if t_separatedb = 1 then --separate dbs
            select num into t_partcheck FROM dblink(t_remote_conn_str, t_cmd) AS remote_part_check(num int);
        else --separate schemas
            select count(*) into t_partcheck from pg_inherits
                JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
                JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
                JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
                JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
            WHERE lower(nmsp_parent.nspname)= lower(t_dbname) and lower(parent.relname) = lower(t_tablename) and lower(child.relname) = t_partitionname
            group by nmsp_parent.nspname, parent.relname, nmsp_child.nspname;
        end if;
        if t_partcheck = 0 or t_partcheck is null then --it needs to be created

            t_addtbs:='';
            if coalesce(t_parttbs,'')<> '' and t_parttbs <> 'pg_default' and t_parttbs <> 'pg_global' then --can't have partition creation specified in these
                t_addtbs:= ' TABLESPACE '||t_parttbs;
            else
                t_addtbs:='';
            end if;

            t_commstat := 'CREATE TABLE '||lower(t_schemaname)||'.'||lower(t_tablename)||'_'||t_yearfrom||'_'||t_monthfrom||' partition of '||lower(t_schemaname)||'.'||lower(t_tablename)||' FOR VALUES FROM ('''||t_yearfrom||'-'||t_monthfrom||'-01'') TO ('''||t_yearto||'-'||t_monthto||'-01'')'||t_addtbs;
            --EXECUTE t_commstat;
            if t_separatedb = 1 then --separate dbs
                PERFORM dblink_exec(t_remote_conn_str, t_commstat);
            else --separate schemas
                EXECUTE replace(t_commstat, t_templateschema||'.', t_dbname||'.');
            end if;
        end if;

    END LOOP;
    --subpartition maintenance loop
    FOR t_schemaname, t_tablename, t_partcolumnname, t_partvalue, t_subpartvalue, t_subparttbs IN (select schemaname, tablename, partcolumnname, partvalue, subpartvalue, subparttbs from admmgt.script_table_partitions p, admmgt.script_tables t where t.id = p.tableid and t.status = 2 and upper(subparttype) = 'RANGE') --only process tables where the definition is completed
    LOOP    


        if lower(t_subpartvalue) = 'month' then --partition by month
            select (date_part('month', t_nextdate))::varchar into t_monthfrom;
            select case when (date_part('month', t_nextdate)+1)::varchar = '13' then '01' else (date_part('month', t_nextdate)+1)::varchar end into t_monthto;

            select (date_part('year', t_nextdate))::varchar into t_yearfrom;
            select case when (date_part('month', t_nextdate)+1)::varchar = '13' then (date_part('year', t_nextdate)+ 1)::varchar else (date_part('year', t_nextdate))::varchar end into t_yearto;

        elsif lower(t_subpartvalue) = 'quarter' then
            select case when (date_part('month', t_nextdate)) < 4 then '1'
                when (date_part('month', t_nextdate)) < 7 then '4'
                when (date_part('month', t_nextdate)) < 10 then '7'
                when (date_part('month', t_nextdate)) <= 12 then '10' end into t_monthfrom;
            select case when t_monthfrom = '1' then '4'
                when t_monthfrom = '4' then '7'
                when t_monthfrom = '7' then '10'
                when t_monthfrom = '10' then '1' end into t_monthto;
            select (date_part('year', t_nextdate))::varchar into t_yearfrom;
            select case when t_monthto = '1' then (date_part('year', t_nextdate)+1)::varchar else (date_part('year', t_nextdate))::varchar end into t_yearto;

        elsif lower(t_subpartvalue) = 'year' then
            t_monthfrom := '1';
            t_monthto := '1';
            select (date_part('year', t_nextdate))::varchar into t_yearfrom;
            select (date_part('year', t_nextdate)+1)::varchar into t_yearto;

        end if;

        --convert month to 2 chars
        select case when length(t_monthto) = 1 then '0'||t_monthto else t_monthto end into t_monthto;
        select case when length(t_monthfrom) = 1 then '0'||t_monthfrom else t_monthfrom end into t_monthfrom;

        --now lets set what the name of the partition should be
        t_partitionname := lower(t_tablename)||'_'||lower(t_partcolumnname)||'_'||lower(t_partvalue);
        t_subpartitionname := t_partitionname||'_'||t_yearfrom||'_'||t_monthfrom;
        t_partcheck := null;
        t_cmd := 'select count(*) as num from pg_inherits
                JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
                JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
                JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
                JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
            WHERE lower(nmsp_parent.nspname)= '''||lower(t_schemaname)||''' and lower(parent.relname) = '''||lower(t_partitionname)||''' and lower(child.relname) = '''||t_subpartitionname||'''
            group by nmsp_parent.nspname, parent.relname, nmsp_child.nspname';
        if t_separatedb = 1 then --separate dbs
            select num into t_partcheck FROM dblink(t_remote_conn_str, t_cmd) AS remote_part_check(num int);
        else --separate schemas
            select count(*) into t_partcheck from pg_inherits
                JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
                JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
                JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
                JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
            WHERE lower(nmsp_parent.nspname)= lower(t_dbname) and lower(parent.relname) = lower(t_partitionname) and lower(child.relname) = t_subpartitionname
            group by nmsp_parent.nspname, parent.relname, nmsp_child.nspname;
        end if;
        if t_partcheck = 0 or t_partcheck is null then --it needs to be created

            t_addtbs:='';
            if coalesce(t_subparttbs,'')<> '' and t_subparttbs <> 'pg_default' and t_subparttbs <> 'pg_global' then --can't have partition creation specified in these
                t_addtbs:= ' TABLESPACE '||t_subparttbs;
            else
                t_addtbs:='';
            end if;

           
            t_commstat := 'CREATE TABLE '||lower(t_schemaname)||'.'||t_subpartitionname||' partition of '||lower(t_schemaname)||'.'||t_partitionname||' FOR VALUES FROM ('''||t_yearfrom||'-'||t_monthfrom||'-01'') TO ('''||t_yearto||'-'||t_monthto||'-01'')'||t_addtbs;
            --EXECUTE t_commstat;
            if t_separatedb = 1 then --separate dbs
                PERFORM dblink_exec(t_remote_conn_str, t_commstat);
            else --separate schemas
                EXECUTE replace(t_commstat, t_templateschema||'.', t_dbname||'.');
            end if;
        end if;

    END LOOP;
END;
$BODY$;


CREATE OR REPLACE PROCEDURE admmgt.createForeignKey(t_separatedb IN integer, t_scriptid IN INT, t_remote_conn_str IN TEXT)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    t_schemaname varchar(128);
    t_childtablename varchar(128);
    t_parenttablename varchar(128);
    t_commstat TEXT;
    t_dbname varchar(128);
    t_templateschema varchar(128);

BEGIN
    --get the name of the DB this will be executed on for logging
    t_dbname := lower(trim(replace(substr(t_remote_conn_str, 1, position( 'user='  in t_remote_conn_str) - 1), 'dbname=', '')));
    --get template schema name
    select lower(dbname) into t_templateschema from admmgt.vendor_db_settings where istemplate = true;
  
   	FOR t_schemaname, t_childtablename, t_parenttablename, t_commstat IN (select lower(schemaname), lower(childtablename), lower(parenttablename),
             'alter table '||schemaname||'.'||childtablename||' add constraint fk_'||childtablename||'_'||parenttablename||' FOREIGN KEY ('||string_agg(childcolumnname,', 'order by childtablename, keyorder)||') REFERENCES '||schemaname||'.'||parenttablename||'('||string_agg(parentcolumnname,', ' order by childtablename, keyorder) ||')'
              ||case when coalesce(delrule <> '') then ' ON DELETE '||delrule else '' end
              ||case when coalesce(novalidate, 0)  = 1 then ' NOT VALID' else  '' end
              from  admmgt.script_table_fkeys where status = 1 and scriptid = t_scriptid
              group by schemaname, childtablename, parenttablename, delrule, novalidate) 
   	LOOP

       	BEGIN 

            if t_separatedb = 1 then --separate dbs
                --log the statement
                insert into admmgt.script_log(scriptid, dbname, commstat) values (t_scriptid,t_dbname, t_commstat); 

                PERFORM dblink_exec(t_remote_conn_str, t_commstat);
            else -- separate schemas
                --log the statement
                insert into admmgt.script_log(scriptid, dbname, commstat) values (t_scriptid,t_dbname, replace(t_commstat, t_templateschema||'.', t_dbname||'.')); 
                EXECUTE replace(t_commstat, t_templateschema||'.', t_dbname||'.');
            end if;
       	END;
   	END LOOP;
END;
$BODY$;           

CREATE OR REPLACE PROCEDURE admmgt.addColumns(t_separatedb IN integer, t_scriptid IN INT, t_remote_conn_str IN TEXT)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    t_pk bigint;
    t_commstat TEXT;
    t_dbname varchar(128);
    t_templateschema varchar(128);

BEGIN
    --get the name of the DB this will be executed on for logging
    t_dbname := lower(trim(replace(substr(t_remote_conn_str, 1, position( 'user='  in t_remote_conn_str) - 1), 'dbname=', '')));
    --get template schema name
    select lower(dbname) into t_templateschema from admmgt.vendor_db_settings where istemplate = true;

    FOR t_pk, t_commstat IN (select c.id, 'alter table '||lower(t.schemaname)||'.'||lower(t.tablename)||' add '||columnname||' '||datatype||' '||case when defaultval is not null then ' default '|| defaultval else '' end ||case when nullable = 0 and isidentity = 0 then ' not null ' else '' end|| case when isidentity = 1 then ' generated always as identity ' else '' end, ','
	    from admmgt.script_tables t, admmgt.script_table_columns c where c.scriptid = t_scriptid and c.tableid = t.id and t.status = 2 and c.status = 1) --only process columns where tables are created and columns are not
    LOOP

        BEGIN 
            if t_separatedb = 1 then --separate dbs
                --log the statement
                insert into admmgt.script_log(scriptid, dbname, commstat) values (t_scriptid,t_dbname, t_commstat); 

                PERFORM dblink_exec(t_remote_conn_str, t_commstat);
            else-- separate schemas
                --log the statement
                insert into admmgt.script_log(scriptid, dbname, commstat) values (t_scriptid,t_dbname, replace(t_commstat, t_templateschema||'.', t_dbname||'.')); 
                EXECUTE replace(t_commstat, t_templateschema||'.', t_dbname||'.');
            end if;
        END;
    END LOOP;
END;
$BODY$;        



CREATE OR REPLACE PROCEDURE admmgt.execSPs(t_separatedb IN integer, t_scriptid IN INT, t_remote_conn_str IN TEXT)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    t_pk bigint;
    t_commstat TEXT;
    t_dbname varchar(128);
    t_templateschema varchar(128);

BEGIN
    --get the name of the DB this will be executed on for logging
    t_dbname := lower(trim(replace(substr(t_remote_conn_str, 1, position( 'user='  in t_remote_conn_str) - 1), 'dbname=', '')));
    --get template schema name
    select lower(dbname) into t_templateschema from admmgt.vendor_db_settings where istemplate = true;

    FOR t_pk, t_commstat IN (select id, proccallstmt from admmgt.script_procs where scriptid = t_scriptid and status = 1) 
    LOOP

        BEGIN 
            if t_separatedb = 1 then --separate dbs
                --log the statement
                insert into admmgt.script_log(scriptid, dbname, commstat) values (t_scriptid,t_dbname, t_commstat); 

                PERFORM dblink_exec(t_remote_conn_str, t_commstat);
            else -- separate schemas
                --log the statement
                insert into admmgt.script_log(scriptid, dbname, commstat) values (t_scriptid,t_dbname, replace(t_commstat, t_templateschema||'.', t_dbname||'.')); 
                EXECUTE replace(t_commstat, t_templateschema||'.', t_dbname||'.');
            end if;
        END;
    END LOOP;
END;
$BODY$;        


--this is the procedure to control applying scripts
CREATE OR REPLACE PROCEDURE admmgt.applyScripts(t_separatedb IN integer)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    t_scriptid int;
    t_dbname varchar;
    t_dbid bigint;
    t_remote_conn_str TEXT;
    t_check int;

BEGIN
    --script loop
    --need to find scripts in sequence that are ready to be applied (status = 1)
    FOR t_scriptid in (select id from admmgt.scripts where status = 1 order by id)
    LOOP
        call admmgt.refesh_stored_procedures(t_separatedb, t_scriptid); --refresh stored procedures on tenants and backup definitions
        --DB loop
        --list of databases ready to have scripts applied
        FOR t_dbname, t_dbid in (select lower(dbname), id from admmgt.vendor_db_settings where status = 2 and updateflag = true and coalesce(scriptversion,0) < t_scriptid order by istemplate desc)
        LOOP

            t_remote_conn_str := 'dbname='||t_dbname||' user=postgres password=your-password port = 5432';
            --this procedure will create tables linked to a script on the current DB
            call admmgt.createTables(t_separatedb, t_scriptid, t_remote_conn_str);
            --this procedure will add columns
            call admmgt.addColumns(t_separatedb, t_scriptid, t_remote_conn_str);
            --this procedure will add foreign keys
            call admmgt.createForeignKey(t_separatedb, t_scriptid, t_remote_conn_str);
            --this is to execute stored procedures
            call admmgt.execSPs(t_separatedb, t_scriptid, t_remote_conn_str);
            --now update the db to the current applied script version
            update admmgt.vendor_db_settings set scriptversion = t_scriptid where id = t_dbid;

        commit;
        END LOOP; --DB loop

        --check if created DB's script version level is at least as high as the script that was just applied
        select count(*) into t_check from admmgt.vendor_db_settings where status = 2 and coalesce(scriptversion, 0) < t_scriptid; 
        if t_check = 0 then
            --now that script changes are applied on all created DBs, flag the script as applied
            update admmgt.scripts set status = 2 where id = t_scriptid;
            --flag new tables as created
            update admmgt.script_tables set status = 2 where scriptid = t_scriptid;
            --flag new columns as created
            update admmgt.script_table_columns set status = 2 where status = 1 and scriptid = t_scriptid;
            --flag new foreign keys as created
            update admmgt.script_table_fkeys set status = 2 where status = 1 and scriptid = t_scriptid;
            --flag stored procs as run
            update admmgt.script_procs set status = 2 where status = 1 and scriptid = t_scriptid;
            commit;
        end if;

        call admmgt.refesh_stored_procedures(t_separatedb,t_scriptid); --refresh stored procedures on tenants and backup definitions

    END LOOP; --script loop
END;
$BODY$;      


--this is the procedure to control maintenance of partitioned tables
CREATE OR REPLACE PROCEDURE admmgt.applyMaintenance(t_separatedb IN integer, t_numdays IN INT)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    t_dbname varchar;
    t_dbid bigint;
    t_remote_conn_str TEXT;

BEGIN

    --DB loop
    --list of databases ready to have scripts applied
    FOR t_dbname, t_dbid in (select lower(dbname), id from admmgt.vendor_db_settings where status = 2 and updateflag = true and istemplate = false)
    LOOP

        t_remote_conn_str := 'dbname='||t_dbname||' user=postgres password=your-password port = 5432';
        --this procedure will create tables linked to a script on the current DB
        --!!!!!*****need to add a loop for high values for numdays
        call admmgt.maintainPartitions(t_separatedb, t_numdays, t_remote_conn_str);
        --may want to add some logging

    commit;
    END LOOP; --DB loop

END;
$BODY$;   