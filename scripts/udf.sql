delete jar /root/hive_test/hivetest-1.jar;
add jar /root/hive_test/hivetest-1.jar;
create temporary function nvl as 'com.zem.test.udf.GenericUDFNvl';
select nvl(null,5);
