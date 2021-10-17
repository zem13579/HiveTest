
-- 注册函数
delete jar /root/hive_test_files/udf/udaf/hivetest-1.jar;
add jar /root/hive_test_files/udf/udaf/hivetest-1.jar;
create temporary function myfor
as 'com.zem.test.udtf.GenericUDTFFor';


select myfor(1,4);


show databases ;
use default;

delete jar /root/hive_test_files/udf/udaf/hivetest-1.jar;
add jar /root/hive_test_files/udf/udaf/hivetest-1.jar;
create temporary function parase_book
    as 'com.zem.test.udtf.UDTFBook';

create table bookinfo(info string);

truncate table bookinfo;
insert into bookinfo values ('abcd444-444|cook|zhanghua,lisi');
insert into bookinfo values ('jjjd8921-099|programming book|zem,zzh');

select * from bookinfo;



select parase_book(info) from bookinfo;

