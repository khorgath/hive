set hive.mapred.mode=nonstrict;
set hive.test.mode=true;
set hive.test.mode.prefix=;
set hive.repl.rootdir=${system:test.tmp.dir}/hrepl;

drop table if exists rdump_3;

show databases;

create table rdump_3(emp_id int comment "employee id")
        partitioned by (emp_country string, emp_state string)
        stored as textfile;
load data local inpath "../../data/files/test.dat"
        into table rdump_3 partition (emp_country="us",emp_state="ca");

select count(*) from rdump_3;
select * from rdump_3;

EXPLAIN REPL DUMP default;

REPL DUMP default;

dfs -ls -R ${system:test.tmp.dir}/hrepl/;

EXPLAIN REPL LOAD default.rdump_4 FROM '${system:test.tmp.dir}/hrepl/next/default/rdump_3';

REPL LOAD default.rdump_4 FROM '${system:test.tmp.dir}/hrepl/next/default/rdump_3';

describe extended rdump_4;

select count(*) from rdump_4;
select * from rdump_4;

EXPLAIN REPL LOAD backup FROM '${system:test.tmp.dir}/hrepl/next/';

REPL LOAD backup FROM '${system:test.tmp.dir}/hrepl/next/';

REPL STATUS backup;

use backup;

show tables;

select count(*) from rdump_3;
select * from rdump_3;

drop table rdump_3;

drop table default.rdump_4;

