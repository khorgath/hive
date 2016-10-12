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


REPL DUMP default;

dfs -ls -R ${system:test.tmp.dir}/hrepl/;

drop table rdump_3;

