
-- 注册函数
add jar /root/hive_test_files/udf/udaf/hivetest-1.jar;
create temporary function mycollect
as 'com.zem.test.udaf.GenericeUDAFCollect';

create table collectest (str string,countVal int)
row format delimited fields terminated by '\t' lines terminated by '\n';

load data local inpath '/root/hive_test_files/udf/udaf/afile.txt' into table collectest;

select mycollect(str) from collectest;


