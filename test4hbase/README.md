## TODO:
1.如何使得MapReducer任务在yarn上执行呢?

## MySQL
1.创建数据库语句
    create database student default charset utf8 collate utf8_general_ci;
2.创建表语句
    create table statistic_result( name varchar(100) not null,sum_cnt int not null,primary key (name)) engine=innodb default charset utf8;
3. 插入数据

## HBase
1.创建及查看数据库
    list
2.插入及查看数据
    put 'student','10001','info:name','zhangsan'
    scan 'student'
3.清空表
    truncate 'student'