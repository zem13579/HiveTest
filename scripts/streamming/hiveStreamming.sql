
drop table if exists a;
create table a (col1 int,col2 int)
    row format delimited fields terminated by '\t';

insert into table a values (4,5);
insert into table a values (3,2);

select * from a;
select transform ( col1,col2 )
        using '/bin/cat' as newA,newB
FROM a;

select transform ( col1,col2 )
        using '/bin/cat' as (newA int,newB double)
FROM a;

select transform ( col1,col2 )
        using '/bin/cut -f1' as newa
from a;

select transform ( col1,col2 )
        using '/bin/sed s/4/10' as newa,newb
from a;

add file /root/hive_test_files/streamming/trans_temp.sh;

select transform ( col1 ) using 'trans_temp.sh' as convert from a