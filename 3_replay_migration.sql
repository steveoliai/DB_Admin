--this is the procedure to apply scripts to a database or schema that is set to NOT be updated along with template
--here we need to replay the update scripts in sequence
/*

!!!!!**** if testing in DB per tenant scenario, need to create a DB (as if it was always there)

CREATE DATABASE replaytest;

--connect to replaytest and add this schema:

CREATE SCHEMA mgttest
    AUTHORIZATION postgres;


--insert record in settings. This is needed in both approaches (schema or DB per tenant)
insert into admmgt.vendor_db_settings (dbname,istemplate, status, updateflag) values ('replaytest', false, 2, false);

--if testing for separate schemas:
update admmgt.vendor_db_settings set status = 1 where dbname = 'replaytest';

call admmgt.create_schema(); --to create pending schemas

--*******
--disconnect from replaytest and reconnect to main postgresDB
*/


CREATE OR REPLACE PROCEDURE admmgt.replayScripts(t_separatedb in integer, t_dbname in varchar, t_targetscriptid in integer)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    t_currentscriptid int;
    t_scriptid int;
    t_dbid bigint;
    t_remote_conn_str TEXT;
    t_check int;
    t_commstat TEXT;
    t_templateschema varchar(128);

BEGIN

    --find current script level
    select id, coalesce(scriptversion, 0) into t_dbid, t_currentscriptid from admmgt.vendor_db_settings where upper(dbname) = upper(t_dbname);
    --get connection string
    t_remote_conn_str := 'dbname='||t_dbname||' user=postgres password=your-password port = 5432';
    --get template schema name
    select lower(dbname) into t_templateschema from admmgt.vendor_db_settings where istemplate = true;
    --script loop
    --need to find scripts in sequence that are ready to be applied (status = 1)
    FOR t_scriptid in (select id from admmgt.scripts where id > t_currentscriptid and id <= t_targetscriptid and status = 2 order by id)
    LOOP

        --refresh procedures for the current state of the DB before the scripts started
        FOR t_commstat IN (select commstat from  admmgt.script_proc_log where scriptid = t_scriptid and ispremigration = true order by id) 
        LOOP

            BEGIN 
                if t_separatedb = 1 then --separate dbs
                
                    --log the statement
                    insert into admmgt.script_log(scriptid, dbname, commstat) values (t_scriptid,t_dbname, t_commstat); 

                    PERFORM dblink_exec(t_remote_conn_str, t_commstat);
                else -- separate schema
                    --log the statement
                    insert into admmgt.script_log(scriptid, dbname, commstat) values (t_scriptid,t_dbname, replace(t_commstat, t_templateschema||'.', t_dbname||'.')); 
                    EXECUTE replace(t_commstat, t_templateschema||'.', t_dbname||'.');
                end if;
            END;
        END LOOP;
        commit;
        --replay looged statements for current script
        FOR t_commstat IN (select commstat from admmgt.script_log where scriptid = t_scriptid and upper(trim(dbname)) = upper(trim(t_templateschema)) order by id) 
        LOOP

            BEGIN 
                if t_separatedb = 1 then --separate dbs            
                    --log the statement
                    insert into admmgt.script_log(scriptid, dbname, commstat) values (t_scriptid,t_dbname, t_commstat); 

                    PERFORM dblink_exec(t_remote_conn_str, t_commstat);
                else -- separate schema
                    --log the statement
                    insert into admmgt.script_log(scriptid, dbname, commstat) values (t_scriptid,t_dbname, replace(t_commstat, t_templateschema||'.', t_dbname||'.')); 
                    EXECUTE replace(t_commstat, t_templateschema||'.', t_dbname||'.');
                end if;

            END;
        END LOOP;
        commit;
        --refresh procedures for the current state of the DB after the scripts completed
    
        FOR t_commstat IN (select commstat from  admmgt.script_proc_log where scriptid = t_scriptid and ispremigration = false order by id) 
        LOOP

            BEGIN 
                if t_separatedb = 1 then --separate dbs            
                    --log the statement
                    insert into admmgt.script_log(scriptid, dbname, commstat) values (t_scriptid,t_dbname, t_commstat); 

                    PERFORM dblink_exec(t_remote_conn_str, t_commstat);
                else -- separate schema
                    --log the statement
                    insert into admmgt.script_log(scriptid, dbname, commstat) values (t_scriptid,t_dbname, replace(t_commstat, t_templateschema||'.', t_dbname||'.')); 
                    EXECUTE replace(t_commstat, t_templateschema||'.', t_dbname||'.');
                end if;
            END;
        END LOOP;
        commit;
 
        update admmgt.vendor_db_settings set scriptversion = t_scriptid where id = t_dbid;

    END LOOP; --script loop
END;
$BODY$;     


--to test
--call admmgt.replayScripts(1, 'replaytest', 4) --for DB per tenant scenario