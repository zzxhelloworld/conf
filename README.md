# Introduction
Here are source codes involving some experiments on study of Composite Object Normal Form(CONF).
Firstly we implement our proposed lossless FD-preserved decomposition algorithm(alg1 at <kbd>conf/CONF.java</kbd>).
Then, we implement 3 algorithms to compare with as our paper introduced, that is, alg2(<kbd>conf/CONF_Comp.java</kbd>), alg3(<kbd>conf/CONF_3NF.java</kbd>) and alg4(<kbd>conf/CONF_Basic.java</kbd>). Besides, we also implement some experiments. For all experiment data and logs, please see <kbd>experiment data/</kbd>
# Data sets
> 1. 14 data sets
>> The datasets are available at https://hpi.de/naumann/projects/repeatability/data-profiling/fds.html
> 2. TPC-H benchmark
>> While for experiments over TPC-H benchmark, you can find data sets stored in databases [here](https://relational.fit.cvut.cz/dataset/TPCH), which is 2G-sized data. You can follow instructions to export TPC-H databases into your own local databases.
# What to need to do before running codes
> 1. setup databases for 14 datasets
>> First, we use MySQL 8.0 as databases software. Then you need create a database called "freeman". Afterwards, import 14 datasets into databases and set column name as 0,1,...,n-1 where n is the number of column of datasets. Also, you need create a column named "id" as an auto_increment attribute for each table.
> 2. TPC-H benchmark
>> As introduced above, you need visit the [website](https://relational.fit.cvut.cz/dataset/TPCH) and export TPC-H database in form of .sql file. Then, just import it into your own local MySQL. Also, you need to visit <kbd>Artifact/02 - Experiments/3 - Performance of n-CONF/TPC-H/</kbd> to get all 22 official sql queries and refresh functions here.
>3. functional dependencies
>> Given the 14 datasets, we compute all atomic FDs that are in <kbd>Artifact/03 - FD/</kbd> and then use if needed. While for TPC-H benchmark, we have set FDs in codes.
>4. JDK & JDBC
>> Our codes were programmed using JAVA. As a consequence, you need specify a JDK with version 8 or later. At the moment, we use JDBC(version 8.0.26) as a connector to access MySQL databases.
# How to use for experiments
Our experiments consist of 5 sub-experiments. For some of sub-experiments, you can run different codes to start, as follows:
>1. Number of Keys on Real-World Schemata
>> In this experiments, we connected to [the relational data
repository](https://relational.fit.cvut.cz) to check how many uniqueness constraints are specified on public data sets. Some results are available in <kbd>Artifact/02 - Experiments/1 - Number of Keys on Real-World Schemata/</kbd>.
>2. Why to minimize 3NF schemata
>> To conduct this experiment, you need set up dataset sources as the lineitem dataset in <kbd>conf/Constant.java</kbd>. Then set some options in main function of <kbd>conf/RealWorldSelectAndUpdate.java</kbd> and finally run the class to start this experiments. We include experimental results in <kbd>Artifact/02 - Experiments/2 - Why to minimize 3NF schemata/</kbd>
> 3. Performance of n-CONF
>> 3.1 Armstrong relations
>>> You can find the code in <kbd>conf/KeyNumExp.java</kbd> and set some variables in main function. After all these done, you can run the class to execute the experiments. For our experimental results and graphs, please see <kbd>Artifact/02 - Experiments/3 - Performance of n-CONF/Armstrong relations/</kbd>.
>> 3.2 TPC-H benchmark
>>> For the TPC-H bencemark experiments, we pre-set some minimal keys in function <kbd>get_table_uniques_from_TPCH()</kbd> of <kbd>additional/KeyNumExpOnTPCBenchmark.java</kbd>. For running, you just need set up some parameters in main funtion and then start. Also, we include some our experimental statistics in <kbd>Artifact/02 - Experiments/3 - Performance of n-CONF/TPC-H/</kbd>.
> 4. Performance of Algorithms
>> 4.1 Real-world
>>> Before running the experiments, we can set some parameters in <kbd>conf/Constant.java</kbd> to access some datasets and to turn on/off other switches. Afterwards, you can run <kbd>conf/Main.java</kbd> to start the experiments. Our results are shown in <kbd>Artifact/02 - Experiments/4 - Performance of Algorithms/Real-world/</kbd>.
>> 4.2 Lineitem
>> 4.3 RunningExample
> 1. main experiments
>> Before running main experiments, we can set some parameters in <kbd>conf/Constant.java</kbd> to access some datasets and to turn on/off other switches. Afterwards, you can run <kbd>conf/Main.java</kbd> to start main experiments.
> 2. synthetic experiments with Armstrong relation
>> You can find the code in <kbd>conf/KeyNumExp.java</kbd> and set some variables in main function. After all these done, you can run the class to execute the experiments.
> 3. real-world experiments to show performance of keys
>> In this experiments, you need set up dataset sources in <kbd>conf/Constant.java</kbd>. Then set some options in main function of <kbd>conf/RealWorldSelectAndUpdate.java</kbd> and finally run the class to start this experiments.
> 4. experiments on TPC-H benchmark
>> For the TPC-H bencemark experiments, we pre-set some minimal keys in function <kbd>get_table_uniques_from_TPCH()</kbd> of <kbd>additional/KeyNumExpOnTPCBenchmark.java</kbd>. For running, you just need set up some parameters in main funtion and then start.
> 5. algorithmic performance tests on Alg.1 and Alg.2 over lineitem dataset
>> The performance comparison over Alg.1 and Alg.2 will be done at <kbd>additional/SubschemaPerfExp.java</kbd>. Before starting experiments, you should only copy decomposition results (from previous <kbd>main experiments</kbd>) that are sub-schemas in BCNF, into a new file, respectively. Then configure the main function and start.
> 6. lossless join decomposition algorithm experiments
>> We implement a classical lossless join decompistion algorithm in <kbd>additional/LosslessJoinDecompIntoBCNF.java</kbd> that is polynomial in cost time and results in BCNF decompositions. Before run, you should set up dataset information in <kbd>conf/Constant.java</kbd> and some other variables in main function of <kbd>additional/LosslessJoinDecompIntoBCNF.java</kbd>. Finally, you can start up the experiments.
