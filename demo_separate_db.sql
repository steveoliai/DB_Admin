
--create template DB
--connect to main postgresDB
--this can not be run in a transaction block
--run create database alone
CREATE DATABASE unitemplate;

--connect to unitemplate and add this schema:

CREATE SCHEMA mgttest
    AUTHORIZATION postgres;

--*******
--disconnect from unitemplate and reconnect to main postgresDB

--insert template record for the DB created above
insert into admmgt.vendor_db_settings (dbname,istemplate, status) values ('unitemplate', true, 2);


--sample data
insert into admmgt.vendor_db_settings (dbname) values ('BigClient');
--setting status to "pending" to the procedure will create it
update admmgt.vendor_db_settings set status = 1 where dbname = 'BigClient';



insert into admmgt.scripts(id, description)  values (1, 'test to create new stuff');

--in the insert below haspart = 1. This is noting that the table which needs to be created is partition
--the value for lifecylce '{"status":"X", "inactive_date":"current_date-100"}' means that anything in this table where status = X and inactive_date < 100 days old is to be removed
--I have another procedure that deals with lifecylce which is comming soon.

insert into admmgt.script_tables (schemaname, scriptid, tablename, status, haspart, lifecycle, tblspace) values ('mgttest', 1, 'test_table', 0, 1, '{"status":"X", "inactive_date":"current_date-100"}', 'pg_default');

--get id generated from insert for later inserts (repalce 1).

--column def
insert into admmgt.script_table_columns (tableid, scriptid, columnname, datatype, nullable, defaultval, isprimarykey, isidentity)
    values (1, 1, 'id', 'bigint', 0, NULL, 1, 1);

insert into admmgt.script_table_columns (tableid, scriptid, columnname, datatype, nullable, defaultval)
    values (1, 1, 'description', 'varchar(256)', 1, NULL);

insert into admmgt.script_table_columns (tableid, scriptid, columnname, datatype, nullable, defaultval)
    values (1, 1, 'code', 'varchar(8)', 1, NULL);    

insert into admmgt.script_table_columns (tableid, scriptid, columnname, datatype, nullable, defaultval)
    values (1, 1, 'status', 'varchar(1)', 0, NULL);

insert into admmgt.script_table_columns (tableid, scriptid, columnname, datatype, nullable, defaultval)
    values (1, 1, 'inactive_date', 'timestamp', 1, NULL);

--parition def
--parttype can be list or range
--supports list, range, and list/range composite partitioning (which is what the inserts below setup)

insert into admmgt.script_table_partitions (tableid, partcolumnname, parttype, partvalue, parttbs)
    values (1, 'status', 'list', 'A', 'pg_default');

insert into admmgt.script_table_partitions (tableid, partcolumnname, parttype, partvalue, subpartcolumnname, subparttype, subpartvalue, parttbs, subparttbs)
    values (1, 'status', 'list', 'X', 'INACTIVE_DATE', 'RANGE', 'quarter', 'pg_default', 'pg_default');

--add a parent and child table
--parent
insert into admmgt.script_tables (schemaname, scriptid, tablename, status, haspart, lifecycle, tblspace) values ('mgttest', 1, 'parenttable', 0, 0, null, 'pg_default');

insert into admmgt.script_table_columns (tableid, scriptid, columnname, datatype, nullable, defaultval, isprimarykey, isidentity)
    values (2, 1, 'id', 'bigint', 0, NULL, 1, 1);

--child
insert into admmgt.script_tables (schemaname, scriptid, tablename, status, haspart, lifecycle, tblspace) values ('mgttest', 1, 'childtable', 0, 0, null, 'pg_default');

insert into admmgt.script_table_columns (tableid, scriptid, columnname, datatype, nullable, defaultval, isprimarykey, isidentity)
    values (3, 1, 'id', 'bigint', 0, NULL, 1, 1);

insert into admmgt.script_table_columns (tableid, scriptid, columnname, datatype, nullable, defaultval, isprimarykey, isidentity)
    values (3, 1, 'parentid', 'bigint', 0, NULL, 0, 0);

--foreign key

insert into admmgt.script_table_fkeys (scriptid, schemaname, childtablename, childcolumnname, parenttablename, parentcolumnname, keyorder, delrule)
    values (1, 'mgttest', 'childtable', 'parentid', 'parenttable', 'id', 1, 'CASCADE');

--once design is complete update the table settings and script rows

update admmgt.script_tables set status = 1 where scriptid = 1;

update admmgt.scripts set status = 1 where id = 1;

update admmgt.script_table_fkeys  set status = 1 where scriptid = 1;

update admmgt.script_table_columns  set status = 1 where scriptid = 1;


call admmgt.create_database(); --to create pending databases

call admmgt.applyScripts(1); --to apply scripts to databases ready to have them applied
--passing 1 above indicates that this is a DB per tenant scenario

call admmgt.applyMaintenance(1, t_numdays => 10); -- to maintain range paritions t_numdays = look ahead
--check BigClient DB, mgttest.test_table partitions after running the applyMaintenance procedure

--check the log for statements that were executed:
select * from admmgt.script_log;


--what if I want to add a column to an existing table?


insert into admmgt.scripts(id, description)  values (2, 'add column to existing table');

--make should tableid value is for the table that was previously created
insert into admmgt.script_table_columns (tableid, scriptid, columnname, datatype, nullable, defaultval, isprimarykey, isidentity)
    values (2, 2, 'newcolumn', 'varchar(10)', 0, NULL, 0, 0);

--set status 

update admmgt.scripts set status = 1 where id = 2;

update admmgt.script_table_columns  set status = 1 where scriptid = 2;

call admmgt.applyScripts(1); --to apply scripts to databases ready to have them applied

--check the results by looking at mgttest.parenttable for "newcolumn"

--how to I apply stored procedures to all tenant databases?

--connect to the template DB (unitemplate)

--add this procedure to the DB

CREATE OR REPLACE PROCEDURE mgttest.testproc1()
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    t_int int;

BEGIN
    t_int := 1;
    WHILE t_int <= 5 LOOP
        insert into mgttest.parenttable(newcolumn) values(t_int::varchar);
        t_int := t_int + 1;
    END LOOP;
END;
$BODY$;   

--disconnect from the template DB

--connect to the main postgresql DB

--!!!!***NOTE this procedure is called as the first step when calleing applyScripts

call admmgt.refesh_stored_procedures(1, 2); --1 = DB per tenant and 2 = pass the id of the latest script entry


--check the BigClient DB for the procedure

--what if this procedure is part of the migration and it makes data changes necessary for the new script version?


insert into admmgt.scripts(id, description)  values (3, 'call stored proc');

insert into admmgt.script_procs(scriptid, proccallstmt) values (3,'call mgttest.testproc1()' );

update admmgt.script_procs set status = 1 where scriptid = 3;

update admmgt.scripts set status = 1 where id = 3;

call admmgt.applyScripts(1); 

--check the mgttest.parenttable in both the template and BigClient DBs to see that there is now data there.



--what if I want to add a JSONB column to an existing table?  Is that supported?
--yes! json is supported.  It's a great way to add flexibility in your datamodel for customers!

insert into admmgt.scripts(id, description)  values (4, 'add jsonb column to existing table');

--make should tableid value is for the table that was previously created
insert into admmgt.script_table_columns (tableid, scriptid, columnname, datatype, nullable, defaultval, isprimarykey, isidentity)
    values (2, 4, 'jsoncolumn', 'jsonb', 1, NULL, 0, 0);

--set status 

update admmgt.scripts set status = 1 where id = 4;

update admmgt.script_table_columns  set status = 1 where scriptid = 4;

call admmgt.applyScripts(1); --to apply scripts to databases ready to have them applied


--can you add a new table and a column to another table in the same script?
insert into admmgt.scripts(id, description)  values (5, 'add a new table and a column to another tablee');

insert into admmgt.script_tables (schemaname, scriptid, tablename, status, haspart, lifecycle, tblspace) values ('mgttest', 5, 'anothertable', 0, 0, null, 'pg_default');

insert into admmgt.script_table_columns (tableid, scriptid, columnname, datatype, nullable, defaultval, isprimarykey, isidentity)
    values (4, 5, 'id', 'bigint', 0, NULL, 1, 1);

--make should tableid value is for the table that was previously created
insert into admmgt.script_table_columns (tableid, scriptid, columnname, datatype, nullable, defaultval, isprimarykey, isidentity)
    values (3, 5, 'nicecolumn', 'jsonb', 1, NULL, 0, 0);

--set status
update admmgt.scripts set status = 1 where id = 5;

update admmgt.script_tables  set status = 1 where scriptid = 5;

update admmgt.script_table_columns  set status = 1 where scriptid = 5;


call admmgt.applyScripts(1); --to apply scripts to databases ready to have them applied
