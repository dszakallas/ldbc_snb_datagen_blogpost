# Speeding up LDBC SNB Datagen

# Introduction

LDBC's [Social Network Benchmark](#snb) (LDBC SNB) is an industrial and academic initiative, formed by principal actors in the field of graph-like data management. Its goal is to define a framework where different graph-based technologies can be fairly tested and compared, that can drive the identification of systems' bottlenecks and required functionalities, and can help researchers to open new research frontiers.

LDBC SNB provides [Datagen](https://github.com/ldbc/ldbc_snb_datagen) (Data Generator), which produces synthetic datasets, mimicing a social network's activity during a period of time. Datagen is defined by the charasteristics of realism, scalability, determinism and usability. To address scalability in particular, Datagen is implemented on the MapReduce platform to enable scaling out to a distributed cluster. Since its inception in the early 2010s, there have been a tremendous amount of development in the big data landscape, so it makes sense reevaluate this decision. More specifically, whether we were better off using Apache Spark instead.

# Overview

The format of this post allows for a minimal introduction only, see [References](#References) for details. The benchmark's specification describes a social network [data model](https://github.com/ldbc/ldbc_snb_docs/blob/dev/figures/schema.pdf), consisting of two parts: static and dynamic. The dynamic part synthesizes a network where people have friends, post in forums, comment or like others' posts, etc.; whereas the static part contains related attributes such as countries, universities and organizations and is unchanged in time. 

Datasets are generated in a multi-stage process shown in the diagram below.

![LDBC SNB Datagen Process on Hadoop](datagen_flow.png)

_Figure 1. LDBC SNB Datagen Process on Hadoop_

In the initialization phase, dictionaries are populated and distributions are initialized. In the first generation phase, persons are synthesized, then relationships are wired between them along 3 dimensions (university, interest and random). After merging the graph of person relationships in HadoopKnowsMerger, the resulting dataset is outputted. Then, activities such as forum posts, comments, likes and photos are generated and outputted. Finally, the static part is outputted.

_Note: The diagram shows the call sequence as implemented. All steps are sequential, including the relationship generation, even when the data dependencies would allow for parallelization, as for these steps._ 

Entities are generated by procedural Java code and represented as POJOs in memory and as sequence files on disk. Most entities follow a shallow representation, i.e foreign keys in the ER model are mapped to integer ids, which makes serialization straightforward.[<sup>1</sup>](#fn1) One exception from this is the `Knows` edge, which contains only the target node, and used as a navigation property on the source `Person`. The target `Person` is replaced with only the foreign key augmented with some additional information, to keep the structure free of cycles. Needless to say, this _edge as property_ representation makes the data more cumbersome to handle in SQL than it would be with a flat join table.

Entity generation amounts to circa one fifth of the main codebase. Most of it involves procedurally generating properties drawing from several random distributions using mutable pRNGs. Determinism is achieved by initializing the pRNGs to seeds that are fully defined by the configuration, and otherwise having no external state.[<sup>2</sup>](#fn2) 

Serialization is done by hand-written serializers for the supported output formats (CSV currently) and comprises just a bit less than one third of the main codebase. Most of the output are created by directly interacting with low-level HDFS file streams. Ideally, this part of the codebase should be migrated to higher level writers that handle faults and give consistent results when the task has to be restarted.

# Motivations for the migration

The application is written using Hadoop MapReduce, which is now largely superseded by the more modern distributed batch processing platforms, notably Apache Spark. For this reason, I proposed to the team to migrate Datagen to Spark, for the following benefits:

**Memory under-utilization** MapReduce is disk-oriented, in the sense that after each reduce stage it writes the output to disk, which is then read by the consecutive MapReduce job. As public clouds provide virtual machines with capable amounts of RAM at an affordable price for this workload, we are wasting time and money by the overhead incurred by this unnecessary disk I/O. Instead, we should cache the intermediate results to memory if possible. The lack of support for this is a well-known limitation of MapReduce.

**Smaller codebase** The Hadoop MapReduce library is fairly ceremonial and boilerplaty. Spark provides a higher-level abstraction that is simpler to work with, while still providing enough control on the lower-level details we required in this workload.

**Small entry cost** Spark uses HDFS under the hood, and the two frameworks are very close conceptually. We can migrate to it with relatively small effort. E.g. we can run MapReduce and Spark jobs on AWS EMR using basically the same hw/sw configuration, which leverages straightforward performance comparisons.

**Incremental improvements** Spark is a platform exposing many APIs for different workloads and operating on different levels of abstraction. While low-level, Java oriented RDDs offer the basic advantages when coming from MapReduce, we can gradually move towards DataFrames to support Parquet output in the serializers and maybe unlock some SQL optimization capabilities in the generators later on.

**OSS, commodity** Spark is one of the most widely used open-source big data platforms, for which every major public cloud provides a managed offering, so the migration increases the approachability and cost-effectiveness. 

# First steps

The first milestone is a successful run of LDBC Datagen on Spark. For this we only want to touch the absolute necessary code paths. These consists of at least the Hadoop wrappers around the generators and serializers. My approach was migrating the generator classes one by one and porting the serializers after them if necessary.

**Regression tests** Lacking tests apart from an id uniqueness check, I had no means to detect bugs introduced by the migration. Designing and implementing a comprehensice test suite was out of scope, so I resorted to regression testing, with the MapReduce output as the baseline. Most of the output are Hadoop sequence files, so I could read them with Spark and compare with the results of the RDD produced by the migrated code.

**Multi-threading issues** Soon after migrating the first generator and running the regression tests, I started to face discrepancies in the output. These only surfaced when I set the parallization level larger than 1. This indicated the presence of potential race conditions. Thread-safety wasn't a concern in the original implementation, due to the fact that MapReduce doesn't use thread-based parallelization for mappers and reducers.[<sup>3</sup>](#fn3) In Spark however, tasks are executed by parallel threads in the same JVM application, so the code is required to be thread safe. After some debugging, I found one bug originating from the shared use of `java.text.SimpleDateFormat` (notoriously known to be not thread-safe) in the serializers. This was resolved simply by changing to `java.time.format.DateTimeFormatter`. There were multiple instances of some static field on an object being mutated concurrently. In some cases this was a temporary buffer and was easily resolved by making it an instance variable. In another case a shared context variable was used, which I resolved by passing dedicated instances as function arguments. Sadly, the Java language has the same syntax for accessing locals, fields and statics, [<sup>4</sup>](#fn4) which makes it somewhat harder to detect the usage scope. Fortunately, in no case I needed to introduce a new mutex or concurrent collection.

**Use your memory!** I focused on keeping the call sequence intact, so that the migrated code evaluates the same steps in the same order, but with the data passed as RDDs. I placed my bet on the hypothesis that we can either cache the required data in memory entirely at all times, or if not, regenerating them is still faster than involving disk I/O loop ptentially incurred by e.g. the `MEMORY_AND_DISK` strategy. This means I used the default caching strategy everywhere.

# Case study: Person ranking

Migrating the majority of the generators was rather straightforward, however the so called person ranking step required some thought. The goal of this step is to organize persons so that similar ones appear close to each other in a deterministic order. This provides a scalabled way to cluster persons according to a similarity metric, as introduced in the [S3G2 paper](#s3g2).

## The original MapReduce version

![Diagram of the MapReduce code for ranking persons](person_ranking.svg)

The implementation is as follows. First, the equivalence keys are mapped to each person and fed into `TotalOrderPartitioner` which maintains an order sensitive partitioning while still trying to emit more or less equal sized groups to keep the data skew low (1). The reducer keys the partitions with its own task id, and a counter variable which has been initialized to zero and incremented on each person, establishing a local ranking inside the group. The final state of the counter (which is the total number of persons in that group) is saved to a separate "side-channel" file upon the completion of a reduce task (2). Then, in a consecutive reduce-only stage, the global order is established by reading all previously emitted count files in order in each reducer and creating an ordered map of each partition number to the corresponding cumulative count of the preceding ones. This is done in the setup phase. In the `reduce` function, the respective count is incremented and assigned to each person (3).

Once this ranking is done, the whole range is sliced up into equally sized blocks, which are processed independently. E.g. when wiring relationships between persons, only those appearing in the same block are considered.

## The migrated version

Spark provides a `sortBy` function which takes care of the first step above in a single line. The gist of the problem remains collecting the partition sizes and making them available in a later step. While the MapReduce version uses a side output, in Spark we can collect the partition sizes in a separate job and pass them into the next phase using a broadcast variable. The resulting code size is a small fraction of the original one.

# Benchmarks

Benchmarks were carried out on [AWS EMR](https://aws.amazon.com/emr). I decided to use High I/O [I3](https://aws.amazon.com/ec2/instance-types/i3/) instances in the beginning because of their fast NVMe SSD storage and capable amount of RAM.


There's a an application parameter `hadoop.numThreads` which controls the number of reduce threads in each Hadoop job for the MapReduce version, while in the Spark version, the number of partitions in the serialization jobs. I set this `n_nodes` in case of Hadoop as larger values caused the jobs to slow down. For the Spark however, increasing it to `n_nodes * v_cpu` yielded some speed. The MapReduce results as follows:

| SF  | workers | Platform  | Instance Type | runtime (min) | runtime * worker/SF (min) |
|-----|---------|-----------|---------------|---------------|---------------------------|
| 10  | 1       | MapReduce | i3.xlarge     | 16            | 1.60                      |
| 30  | 1       | MapReduce | i3.xlarge     | 34            | 1.13                      |
| 100 | 3       | MapReduce | i3.xlarge     | 40            | 1.20                      |
| 300 | 9       | MapReduce | i3.xlarge     | 44            | 1.32                      |


The cost per scale factor only slowly increases, which is good. The metrics charts show an underutilized, bursty CPU. The bursts are supposedly interrupted by the disk I/O parts of writing the results of a completed job. We can also see that the memory only starts to get eaten up when we are already 10 minutes into the run. 

![CPU Load for the Map Reduce cluster is bursty and less than 50% on average (SF_100)](mr_sf100_cpu_load.png)

CPU Load for the Map Reduce cluster is bursty and less than 50% on average (SF100)

![The job only starts to comsume memory when already 10 minutes into the run (SF100)](mr_sf100_mem_free.png)

The job only starts to consume memory when already 10 minutes into the run (SF100)

Let's see how Spark fares:


| SF   | workers | Platform | Instance Type | runtime (min) | runtime/SF/worker (min) |
|------|---------|----------|---------------|---------------|-------------------------|
| 10   | 1       | Spark    | i3.xlarge     | 10            | 1.00                    |
| 30   | 1       | Spark    | i3.xlarge     | 21            | 0.70                    |
| 100  | 3       | Spark    | i3.xlarge     | 27            | 0.81                    |
| 300  | 9       | Spark    | i3.xlarge     | 36            | 1.08                    |
| 1000 | 30      | Spark    | i3.xlarge     | 47            | 1.41                    |
| 3000 | 90      | Spark    | i3.xlarge     | 47            | 1.41                    |

A similar trend here, however the run times are around 70% of the MapReduce version. You can see that the larger scale factors (SF1000 and SF3000), yielded a suprisingly slow runtime. On the metrics charts of SF100, we can see a fully utilized CPU, except in the end, when the results are serialized in one go, and the CPU is basically idle (the snapshot of the diagram doesn't include this part unfortunately). We can see that Spark used up all memory pretty fast even in case of SF100. My hypothesis attributes the slowdowns noticed in SF1000 and SF3000 to the fact that the nodes are running low on memory, and have to calculate RDDs multiple times (no disk level serialization was used here). In fact, I encountered a few OOM errors when running SF3000, so I decided adding some juice (CPU and RAM) to the nodes.

![Full CPU utilization for Spark (SF100)](spark_sf100_cpu_load.png)
Full CPU utilization for Spark (SF100)

![Spark eats up memory fast (SF100)](mr_sf100_mem_free.png)
Spark eats up memory fast (SF100)

i3.2xlarge would have been the most straightforward option for scaling up the instances, however the humongous 1.9 TB disk of this image is completely superflouos for the job. I decided to go with the cheaper r5d.2xlarge instance, largely identical to i3.2xlarge, except it _only_ has a 300 GB SSD.


| SF   | workers | Platform | Instance Type | runtime (min) | runtime * worker/SF (min) |
|------|---------|----------|---------------|---------------|---------------------------|
| 100  | 3       | Spark    | r5d.2xlarge   | 16            | 0.48                      |
| 300  | 9       | Spark    | r5d.2xlarge   | 21            | 0.63                      |
| 1000 | 30      | Spark    | r5d.2xlarge   | 26            | 0.78                      |
| 3000 | 90      | Spark    | r5d.2xlarge   |               |                           |


# Next steps

The next improvement is refactoring the serializers so they use Spark's high-level writer facilities. The most compelling benefit is that it will make the jobs fault-tolerant, as Spark's writers make sure that the integrity of the output is kept in face of task failures. This makes Datagen more resilient and opens up the possibility to run on less reliable hardware configuration (EC2 spot nodes on AWS) for additional cost savings. They will supposedly also yield some speedup on the same cluster configuration. 

# Acknowledgements

This work is based upon the work of Arnau Prat, Gábor Szárnyas, Ben Steer, Jack Waudby and other LDBC contributors. Thanks for your help and feedback!

# References

- <a name="datagen"></a>[9th TUC Meeting – LDBC SNB Datagen Update – Arnau Prat (UPC)](https://www.youtube.com/watch?v=ZQOLuCOOpSI) - [slides](http://wiki.ldbcouncil.org/pages/viewpage.action?pageId=59277315&preview=/59277315/75431942/datagen_in_depth.pdf)
- <a name="s3g2"></a>[S3G2: a Scalable Structure-correlated Social Graph Generator](https://research.vu.nl/en/publications/s3g2-a-scalable-structure-correlated-social-graph-generator)
- <a name="snb"></a>[The LDBC Social Network Benchmark](https://arxiv.org/abs/2001.02299)
- [LDBC](http://www.ldbcouncil.org/) - [GitHub organization](https://github.com/ldbc)

<a name="fn1"><sup>1</sup></a> Also makes it easier to map to a tabular format thus it is a SQL friendly representation.

<a name="fn2"><sup>2</sup></a> It's hard to imagine this done declaratively in SQL.

<a name="fn3"><sup>3</sup></a> Instead, multiple YARN containers have to be used if you want to parallelize on the same machine. 

<a name="fn4"><sup>4</sup></a> Although editors usually render these using different font styles.

