
--supports 3 lifecycle column conditions
CREATE OR REPLACE PROCEDURE admmgt.lifecycleMgt()
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    t_buildstat varchar(4000);
    t_commstat TEXT;
    t_value varchar(128);
    t_lccol1 varchar(128);
    t_lctype1 varchar(128);
    t_lcval1 varchar(128);
    t_lccol2 varchar(128);
    t_lctype2 varchar(128);
    t_lcval2 varchar(128);
    t_lccol3 varchar(128);
    t_lctype3 varchar(128);
    t_lcval3 varchar(128);
    t_lccount int;
    t_partdefcheck int;
    t_partcol varchar(128);
    t_partvalue varchar(128);
    t_subpartcol varchar(128);
    t_subpartvalue varchar(128);
    t_lcdate date;
    t_partitionname varchar(128);
    t_subpartitionname varchar(128);
    t_idxrebuild int;
    t_partcheck int;
    t_monthfrom varchar(2);
    t_yearfrom varchar(4);
    --for delete statements
    t_lccolfmt1 varchar(128);
    t_lccolfmt2 varchar(128);
    t_lccolfmt3 varchar(128);
    t_lcvalcond1 varchar(128);
    t_lcvalcond2 varchar(128);
    t_lcvalcond3 varchar(128);
    t_tabrec_id bigint;

    t_tabrec_schemaname varchar(128); 
    t_tabrec_tablename varchar(128);
    t_tabrec_lifecycle jsonb;
    t_colrec_columnname varchar(128);
    t_colrec_datatype varchar(128);
    t_oldpartrec_partname  varchar(128);

BEGIN

    --partition maintenance loop
    FOR t_tabrec_id, t_tabrec_schemaname, t_tabrec_tablename, t_tabrec_lifecycle IN (select id, schemaname, tablename, lifecycle from admmgt.script_tables t where t.status = 2 and lifecycle is not null) --only process tables where the definition is completed
    LOOP

        t_lccol1 := null;
        t_lctype1 := null;
        t_lcval1 := null;
        t_lccol2 := null;
        t_lctype2 := null;
        t_lcval2 := null;
        t_lccol3 := null;
        t_lctype3 := null;
        t_lcval3 := null;

        t_lccount := 0;
        --start checking defined columns against lifecycle key columns
        FOR t_colrec_columnname, t_colrec_datatype in (select lower(columnname) as columnname, datatype from admmgt.script_table_columns where tableid = t_tabrec_id order by id)
        LOOP
            t_value := null;
            t_value := t_tabrec_lifecycle->>t_colrec_columnname;

            if t_value is not null then --this column is part of lifecycle    
                if t_lccount < 3 then --start keeping track of number of columns (only support for 3 currently)
                    t_lccount := t_lccount + 1;
                    if t_lccount = 1 then --this is the first column
                        t_lccol1 := t_colrec_columnname;
                        t_lctype1 := t_colrec_datatype;
                        t_lcval1 := t_value;
                    elsif t_lccount = 2 then --this is the first column
                        t_lccol2 := t_colrec_columnname;
                        t_lctype2 := t_colrec_datatype;
                        t_lcval2 := t_value;
                    elsif t_lccount = 3 then --this is the first column
                        t_lccol3 := t_colrec_columnname;
                        t_lctype3 := t_colrec_datatype;
                        t_lcval3 := t_value;    
                    end if;
                end if;
            end if;
        END LOOP;

        --now we know the lifecylcle columns and conditions.  let's check for partition definition matches
        select count(*) into t_partdefcheck from admmgt.script_table_partitions where tableid = t_tabrec_id;
        --the number of rows (count above) lets us know a lot. 
        --if no rows - no partitions. if one row, partition based on date & no subpartition. if two or more, can list OR list/range partition
        if t_partdefcheck = 1 and t_lccount = 1 then --check if partition column and lifecycle column (should only be one)
            --make sure the are the same column
            select lower(partcolumnname), lower(partvalue) into t_partcol, t_partvalue from admmgt.script_table_partitions where tableid = t_tabrec_id;

--1 single column date range partiion            

            if t_partcol = t_lccol1 and lower(t_lctype1) in ('date', 'timestamp') then -- they are the same and correct data type
                --now we need to check if there's a partition that only has data older than lifecycle policy 
                --to do that, we need to find the name of the parition that would store the data for the end of lifecyle

                --get the date to keep from the lifeclycle
                EXECUTE 
                    'select '||t_lcval1 into t_lcdate;

                --need to get month and year values for parition name

                if lower(t_partvalue) = 'month' then --partition by month
                    select (date_part('month', t_lcdate))::varchar into t_monthfrom;
                    select (date_part('year', t_lcdate))::varchar into t_yearfrom;
        
                elsif lower(t_partvalue) = 'quarter' then
                    select case when (date_part('month', t_lcdate)) < 4 then '1'
                        when (date_part('month', t_lcdate)) < 7 then '4'
                        when (date_part('month', t_lcdate)) < 10 then '7'
                        when (date_part('month', t_lcdate)) <= 12 then '10' end into t_monthfrom;
                    select (date_part('year', t_lcdate))::varchar into t_yearfrom;
        
                elsif lower(t_partvalue) = 'year' then
                    t_monthfrom := '1';
                    select (date_part('year', t_lcdate))::varchar into t_yearfrom;
                end if;

                --convert month to 2 chars
                select case when length(t_monthfrom) = 1 then '0'||t_monthfrom else t_monthfrom end into t_monthfrom;

                t_partitionname := t_tabrec_tablename||'_'||t_yearfrom||'_'||t_monthfrom;

                --now we have the name, let's drop any older paritions
                
                FOR t_oldpartrec_partname in ( select child.relname from pg_inherits
                    JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
                    JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
                    JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
                    JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
                    WHERE lower(nmsp_parent.nspname)= lower(t_tabrec_schemaname) and lower(parent.relname) = lower(t_tabrec_tablename) and lower(child.relname) < t_partitionname
                    group by nmsp_parent.nspname, parent.relname, nmsp_child.nspname, child.relname)
                LOOP   

                    t_commstat := 'DROP TABLE '||t_tabrec_schemaname||'.'||t_oldpartrec_partname;
                    --insert into admmgt.script_log(commstat) values (t_commstat); --for debugging
                    EXECUTE t_commstat; 
                    
                END LOOP;
 
            end if;

        --end if;

--2 composite list/range parition
        
        elsif t_partdefcheck > 1 and t_lccount = 2 then --could be more than 2 rows in partition def.

            select count(*) into t_partcheck from admmgt.script_table_partitions where tableid = t_tabrec_id
                and lower(partcolumnname) in (t_lccol1, t_lccol2)
                and lower(subpartcolumnname) in (t_lccol1, t_lccol2);

            if t_partcheck = 1 then -- there is a partition def that matches life cycle

                --now we need to check if there's a partition that only has data older than lifecycle policy 
                --to do that, we need to find the name of the SUBparition that would store the data for the end of lifecyle

                --only list/range(date) scheme is supported-- so we can make assumptions
                select lower(partcolumnname), lower(partvalue), lower(subpartcolumnname), lower(subpartvalue)  into t_partcol, t_partvalue, t_subpartcol, t_subpartvalue from admmgt.script_table_partitions where tableid = t_tabrec_id
                    and lower(partcolumnname) in (t_lccol1, t_lccol2)
                    and lower(subpartcolumnname) in (t_lccol1, t_lccol2);


                t_partitionname := t_tabrec_tablename||'_'||t_partvalue;
                if t_lcval1 like 'current%' then
                --get the date to keep from the lifeclycle
                    EXECUTE 
                        'select '||t_lcval1 into t_lcdate;
                elsif t_lcval2 like 'current%' then
                    EXECUTE 
                        'select '||t_lcval2 into t_lcdate;
                end if;


                --need to get month and year values for parition name

                if lower(t_subpartvalue) = 'month' then --partition by month
                    select (date_part('month', t_lcdate))::varchar into t_monthfrom;
                    select (date_part('year', t_lcdate))::varchar into t_yearfrom;
        
                elsif lower(t_subpartvalue) = 'quarter' then
                    select case when (date_part('month', t_lcdate)) < 4 then '1'
                        when (date_part('month', t_lcdate)) < 7 then '4'
                        when (date_part('month', t_lcdate)) < 10 then '7'
                        when (date_part('month', t_lcdate)) <= 12 then '10' end into t_monthfrom;
                    select (date_part('year', t_lcdate))::varchar into t_yearfrom;
        
                elsif lower(t_subpartvalue) = 'year' then
                    t_monthfrom := '1';
                    select (date_part('year', t_lcdate))::varchar into t_yearfrom;
                end if;

                --convert month to 2 chars
                select case when length(t_monthfrom) = 1 then '0'||t_monthfrom else t_monthfrom end into t_monthfrom;

                --now lets set what the name of the sub/partition should be
                t_partitionname := lower(t_tabrec_tablename)||'_'||lower(t_partcol)||'_'||lower(t_partvalue);
                t_subpartitionname := t_partitionname||'_'||t_yearfrom||'_'||t_monthfrom;

                --now we have the names, let's drop any older paritions
                                
                FOR t_oldpartrec_partname in ( select child.relname from pg_inherits
                    JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid
                    JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid
                    JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace
                    JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace
                    WHERE lower(nmsp_parent.nspname)= lower(t_tabrec_schemaname) and lower(parent.relname) = lower(t_partitionname) and lower(child.relname) < t_subpartitionname
                    group by nmsp_parent.nspname, parent.relname, nmsp_child.nspname, child.relname)
                LOOP   

                    t_commstat := 'DROP TABLE '||t_tabrec_schemaname||'.'||t_oldpartrec_partname;
                    --insert into admmgt.script_log(commstat) values (t_commstat); --for debugging
                    EXECUTE t_commstat; 
                    
                END LOOP;


            end if;
        
        else  -- no matching partition definition so build a delete statement
            --depending on data type may need to add some formatting for passing conditions

            --first condition
            if lower(t_lctype1) like 'varchar%(1)' then -- this is a flag column and value will be stored as upper with no Function Index
                t_lcvalcond1:= ' = '''||upper(t_lcval1)||'''';
                t_lccolfmt1 := t_lccol1;
            elsif lower(t_lctype1) like 'varchar%' then --need to convert both val and column condition to use upper     
                t_lcvalcond1:= upper(t_lcval1);
                t_lccolfmt1 := ' = upper('''||t_lccol1||''')';
            elsif lower(t_lctype1) like 'date%' or lower(t_lctype1) like 'timestamp%' then -- assume a function is passes in the condition (like: < today - 100 days)
                t_lcvalcond1:= ' < ('||upper(t_lcval1)||')';
                t_lccolfmt1 := t_lccol1;
            else -- just pass as-is
                t_lcvalcond1:= ' = '||t_lcval1;
                t_lccolfmt1 := t_lccol1;
            end if;

            --second condition
            if lower(t_lctype2) like 'varchar%(1)' then -- this is a flag column and value will be stored as upper with no Function Index
                t_lcvalcond2:= ' = '''||upper(t_lcval2)||'''';
                t_lccolfmt2 := t_lccol2;
            elsif lower(t_lctype2) like 'varchar%' then --need to convert both val and column condition to use upper     
                t_lcvalcond2:= upper(t_lcval2);
                t_lccolfmt2 := ' = upper('''||t_lccol2||''')';
            elsif lower(t_lctype2) like 'date%' or lower(t_lctype2) like 'timestamp%' then -- assume a function is passes in the condition (like: today - 100 days)
                t_lcvalcond2:= ' < ('||upper(t_lcval2)||')';
                t_lccolfmt2 := t_lccol2;
            else -- just pass as-is
                t_lcvalcond2:= ' = '||t_lcval2;
                t_lccolfmt2 := t_lccol2;
            end if;     

            --third condition
            if lower(t_lctype3) like 'varchar%(1)' then -- this is a flag column and value will be stored as upper with no Function Index
                t_lcvalcond3:= ' = '''||upper(t_lcval3)||'''';
                t_lccolfmt3 := t_lccol3;
            elsif lower(t_lctype3) like 'varchar%' then --need to convert both val and column condition to use upper     
                t_lcvalcond3:= upper(t_lcval3);
                t_lccolfmt3 := ' = upper('''||t_lccol3||''')';
            elsif lower(t_lctype3) like 'date%' or lower(t_lctype3) like 'timestamp%' then -- assume a function is passes in the condition (like: today - 100 days)
                t_lcvalcond3:= ' < ('||upper(t_lcval3)||')';
                t_lccolfmt3 := t_lccol3;
            else -- just pass as-is
                t_lcvalcond3:= ' = '||t_lcval3;
                t_lccolfmt3 := t_lccol3;
            end if;                      

            if t_lccount = 1 then --just one condition
                t_commstat := 'DELETE from '||t_tabrec_schemaname||'.'||t_tabrec_tablename||' where '||t_lccolfmt1||t_lcvalcond1;
            elsif t_lccount = 2 then -- two conditions
                t_commstat := 'DELETE from '||t_tabrec_schemaname||'.'||t_tabrec_tablename||' where '||t_lccolfmt1||t_lcvalcond1||' and '||t_lccolfmt2||t_lcvalcond2;
            elsif t_lccount = 3 then -- three(max) conditions
                t_commstat := 'DELETE from '||t_tabrec_schemaname||'.'||t_tabrec_tablename||' where '||t_lccolfmt1||t_lcvalcond1||' and '||t_lccolfmt2||t_lcvalcond2||' and '||t_lccolfmt3||t_lcvalcond3;
            end if;
            --insert into admmgt.script_log(commstat) values (t_commstat); --for debugging
            EXECUTE t_commstat; 
            --commit;
        end if;

    END LOOP;
END;
$BODY$;
