# FFM_training_data
Prepare training data for field-aware factorization machine (FFM).
Using pyspark and data is stored in HDFS.
## Generate Data for specific time interval
Under folder specific_interval, command line:
. `sh run.sh arg1 arg2`
. parameter's formate is %Y%m%d.
For example:
. `sh run.sh 20180615 20180625`

