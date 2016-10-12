set hive.mapred.mode=nonstrict;
set hive.test.mode=true;
set hive.test.mode.prefix=;
set hive.test.mode.nosamplelist=replmt,repl_mt_imported,repl_mt_r_imported;

drop table if exists replmt;
drop table if exists replmt_imported;
drop table if exists replmt_r_imported;

create table replmt (emp_id int comment "employee id")
        partitioned by (emp_country string, emp_state string)
        stored as textfile;
load data local inpath "../../data/files/test.dat"
        into table replmt partition (emp_country="us",emp_state="ca");


dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/replmt/temp;
dfs -rmr target/tmp/ql/test/data/exports/replmt;
dfs ${system:test.dfs.mkdir} target/tmp/ql/test/data/exports/replmt_r/temp;
dfs -rmr target/tmp/ql/test/data/exports/replmt_r;

export table replmt to 'ql/test/data/exports/repl_mt';

dfs -ls -R target/tmp/ql/test/data/exports/repl_mt/;

export table replmt to 'ql/test/data/exports/repl_mt_r' for replication('repl_mt_r');

dfs -ls -R target/tmp/ql/test/data/exports/repl_mt_r/;

-- dfs -cat target/tmp/ql/test/data/exports/repl_mt_r/_files;
-- dfs -cat target/tmp/ql/test/data/exports/repl_mt_r/emp_country=us/emp_state=ca/_files;

import table replmt_imported from 'ql/test/data/exports/repl_mt';
describe extended replmt_imported;
show table extended like replmt_imported;
show create table replmt_imported;
select * from replmt_imported;

-- should have repl.last.id
import table replmt_r_imported from 'ql/test/data/exports/repl_mt_r';
describe extended replmt_r_imported;
show table extended like replmt_r_imported;
show create table replmt_r_imported;
select * from replmt_r_imported;

drop table replmt;

drop table replmt_imported;
drop table replmt_r_imported;
