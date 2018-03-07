# Description
### Prerequisites
Download and install [Apache Spark 2.2.1](https://spark.apache.org/downloads.html).
### Implementation
1. Download *Xuehan_Zhao_HW1.zip* and uncompress it. Copy the two *.jar* files and paste them into the *"/bin"* folder under your **Spark** directory. (e.g. ./spark-2.2.1-bin-hadoop2.7/bin). Make sure your also include the *reviews_Toys_and_Games_5.json* and *metadata.json* in *"/bin"*!
2. In command line, change your working directory to *"/bin"*,run the command below to get my result *Xuehan_Zhao_result_task1.csv* file for task 1:
```
$ ./spark-submit --class Xuehan_Zhao_task1 Xuehan_Zhao_task1.jar reviews_Toys_and_Games_5.json Xuehan_Zhao_result_task1.csv
```
3. Then run the command below to get my result *Xuehan_Zhao_result_task2.csv* file for task 2:
```
$ ./spark-submit --class Xuehan_Zhao_task2 Xuehan_Zhao_task2.jar reviews_Toys_and_Games_5.json metadata.json Xuehan_Zhao_result_task1.csv
```
