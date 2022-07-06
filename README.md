# Introduction
Here are source codes involving some experiments on study of Composite Object Normal Form(CONF).
Firstly we implement our proposed lossless FD-preserved decomposition algorithm(alg1 at conf.CONF.java).
Then, we implement 3 algorithms to compare with as our paper introduced, that is, alg2(conf.CONF_Comp.java), alg3(conf.CONF_3NF.java) and alg4(conf.CONF_Basic.java).
# Data set
> 1. main experiment
>> For the main experiments,we compare 4 algorithms over 14 datasets. The datasets are available at https://hpi.de/naumann/projects/repeatability/data-profiling/fds.html
> 2. TPC-H benchmark experiment
>> While for experiments over TPC-H benchmark, you can find data sets stored in databases with the link(https://relational.fit.cvut.cz/dataset/TPCH), which is 2G-sized data. You can follow instructions to export TPC-H databases into your own local databases.
# What to need to do before running code
> 1. setup databases for 14 datasets
>> First, we use MySQL 8.0 as databases software. Then you need create a database call "freeman". Afterwards, import 14 datasets into databases and set column name as 0,1,...,n-1 where n is the number of column of datasets. Also, you need create a column named "id" as a auto_increment attribute for each table.
> 2. import TPC-H benchmark into MySQL
>> As introduced above, you need visit the website and export TPC-H database in form of .sql file. Then, just import it into your own local MySQL.
>3. functional dependencies
>> Once you get datasets, you can leverage some tools to produce atomic FDs.
>4. JDK & JDBC
>> Our codes were programmed using JAVA. As a consequence, you need specify a JDK with version 8 or later. At the moment, we use JDBC(version 8.0.26) as a connector to access MySQL databases.
# How to use
Our experiments consist of 6 sub-experiments, as follows:
> 1. main experiment
> 2. synthetic experiment with Armstrong relation
> 3. real-world experiment to show performance of keys
> 4. experiments on TPC-H benchmark
> 5. algorithmic performance test on Alg.1 and Alg.2 over lineitem dataset
> 6. lossless join decompisition algorithm experiment
