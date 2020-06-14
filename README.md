# scala-spark-example
This is simple code for compute data from parquet file
## How To Run (Step by step)
1. Install `Scala` and `Spark` ([Mac](https://medium.com/beeranddiapers/installing-apache-spark-on-mac-os-ce416007d79f)/[Linux](https://phoenixnap.com/kb/install-spark-on-ubuntu)).
2. Clone this repository.
3. cd `scala-spark-example`
4. Start spark by `spark shell`
5. Type `:load ReadParquetFile.scala` for import and define object   
6. Type `ReadParquetFile.main(Array("URL"))` with URL is path to parquet file
## Result
I push my result to folder `userdata` has numbered 1 to 5
## How to run with spark-submit on local
1. Dowload Spark: https://downloads.apache.org/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz and unpack folder
2. Install JDK8 (required)
3. cd `path/to/spark-2.4.5-bin-hadoop2.7/sbin` and run `./start-master.sh` and copy URL master
4. Run `./start-slave.sh <URL master>`
5. Access `localhost::8080` for manage
![Screen Shot 1](evidence/evidence-1.png)
6. cd `to current project` and run `spark-submit  --master <URL master> --deploy-mode cluster  --class "ReadParquetFile" <path to jar file> <"path to parquet file">`
![Screen Shot 2](evidence/evidence-2.png)
On URL master
![Screen Shot 3](evidence/evidence-3.png)
7. Click to DriverID and click `stdout` to check where result has been stored 
![Screen Shot 4](evidence/evidence-4.png)
![Screen Shot 5](evidence/evidence-5.png)
![Screen Shot 6](evidence/evidence-6.png)
## How to run with spark-submit on 3-Node Hadoop Cluster
1. Install 3-Node Hadoop Cluster and Spark: 
 - https://www.linode.com/docs/databases/hadoop/how-to-install-and-set-up-hadoop-cluster/
 - https://www.linode.com/docs/databases/hadoop/install-configure-run-spark-on-top-of-hadoop-yarn-cluster/
 Note: 
 - Need disable firewall
 - Configuration each node:
    - RAM: 2G
    - HDD: 30GB
    - Cores: 8
 - Put file date to hdfs `hdfs dfs -put <myfile> <mypath>`
 Example: `hdfs dfs -put /scala-spark-example/data_sample/userdata1.parquet datauser`
2. Run `spark-submit  --deploy-mode cluster  --class "ReadParquetFile" <path to jar file> "<path hdfs file data>"
 - Example `spark-submit  --deploy-mode cluster  --class "ReadParquetFile" /opt/scala-spark-example/target/scala-2.12/read-parquet-file_2.12-1.0.jar "hdfs:///user/hadoop/datauser/userdata3.parquet"`
3. Result will save on hdfs of node-master
 - Folder will have same with file name
 ![Screen Shot 7](evidence/evidence-7.png)
 ![Screen Shot 8](evidence/evidence-8.png)
 
