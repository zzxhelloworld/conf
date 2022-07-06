# Introduction
Here are source codes involving some experiments on study of Composite Object Normal Form(CONF).
Firstly we implement our proposed lossless FD-preserved decomposition algorithm(alg1 at <kbd>conf.CONF.java</kbd>).
Then, we implement 3 algorithms to compare with as our paper introduced, that is, alg2(<kbd>conf.CONF_Comp.java</kbd>), alg3(<kbd>conf.CONF_3NF.java</kbd>) and alg4(<kbd>conf.CONF_Basic.java</kbd>).
# Data sets
> 1. main experiment
>> For the main experiments,we compare 4 algorithms over 14 datasets. The datasets are available at https://hpi.de/naumann/projects/repeatability/data-profiling/fds.html
> 2. TPC-H benchmark experiment
>> While for experiments over TPC-H benchmark, you can find data sets stored in databases with the link(https://relational.fit.cvut.cz/dataset/TPCH), which is 2G-sized data. You can follow instructions to export TPC-H databases into your own local databases.
# What to need to do before running codes
> 1. setup databases for 14 datasets
>> First, we use MySQL 8.0 as databases software. Then you need create a database call "freeman". Afterwards, import 14 datasets into databases and set column name as 0,1,...,n-1 where n is the number of column of datasets. Also, you need create a column named "id" as an auto_increment attribute for each table.
> 2. TPC-H benchmark
>> As introduced above, you need visit the [website](https://relational.fit.cvut.cz/dataset/TPCH) and export TPC-H database in form of .sql file. Then, just import it into your own local MySQL. Also, you need to visit [TPC-H official documents](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v3.0.0.pdf) to get all 22 official sql queries and refresh functions there.
>3. functional dependencies
>> Once you get datasets, you can leverage some tools to produce atomic FDs.
>4. JDK & JDBC
>> Our codes were programmed using JAVA. As a consequence, you need specify a JDK with version 8 or later. At the moment, we use JDBC(version 8.0.26) as a connector to access MySQL databases.
# How to use for experiments
Our experiments consist of 6 sub-experiments, as follows:
> 1. main experiments
>> Before running main experiments, we can set some parameters in <kbd>conf.Constant.java</kbd> to access some datasets and to turn on/off other switches. Afterwards, you can run <kbd>conf.Main.java</kbd> to start main experiments.
> 2. synthetic experiments with Armstrong relation
>> You can find the code in <kbd>conf.KeyNumExp.java</kbd> and set some variables in main function. After all these done, you can run the class to execute the experiments.
> 3. real-world experiments to show performance of keys
>> In this experiments, you need set up dataset sources in <kbd>conf.Constant.java</kbd>. Then set some options in main function of <kbd>conf.RealWorldSelectAndUpdate.java</kbd> and finally run the class to start this experiments.
> 4. experiments on TPC-H benchmark
>> For the TPC-H bencemark experiments, we pre-set some minimal keys in function <kbd>get_table_uniques_from_TPCH()</kbd> of <kbd>additional.KeyNumExpOnTPCBenchmark.java</kbd>. For running, you just need set up some parameters in main funtion and then start.
> 5. algorithmic performance tests on Alg.1 and Alg.2 over lineitem dataset
> 6. lossless join decomposition algorithm experiments
