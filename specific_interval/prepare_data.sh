#!/usr/bin/env bash
#!/usr/bin/env bash
#Yi Wei
#check if push event data exists
time_hour=$(date -d "$1" +%Y%m%d%H)
push_event_hdfs="hdfs://dc2/user/mrd/push/event/"${time_hour}"/*"
push_event_hdfs_flag="hdfs://dc2/user/mrd/push/event/"${time_hour}"/_SUCCESS"
hadoop fs -test -e ${push_event_hdfs_flag}
if [ $? -ne 0 ]
then
    echo ${push_event_hdfs}" file not exist"
    exit 2
fi
#check if long_term user profile data exists
#one_day_ago=$(date -d '1 days ago' +'%Y%m%d')
one_day_ago=$(date -d "$1 - 1 day")
one_day_ago=$(date -d "$one_day_ago" +%Y%m%d)
user_long_hdfs="hdfs://dc2/user/mrd/hive/warehouse/userprofile.db/user_profile_long/p_date="${one_day_ago}"/*"
user_long_hdfs_flag="hdfs://dc2/user/mrd/hive/warehouse/userprofile.db/user_profile_long/p_date="${one_day_ago}"/_SUCCESS"
hadoop fs -test -e ${user_long_hdfs_flag}
if [ $? -ne 0 ]
then
    two_days_ago=$(date -d "$1 - 2 days")
    two_days_ago=$(date -d "$two_days_ago" +%Y%m%d)
    echo ${user_long_hdfs}" file not exist"
    user_long_hdfs="hdfs://dc2/user/mrd/hive/warehouse/userprofile.db/user_profile_long/p_date="${two_days_ago}"/*"
fi
#check if short_term user profile data exists
user_short_hdfs="hdfs://dc2/user/mrd/push/ltr/rank/"${one_day_ago}"/*/*/*"
user_short_hdfs_flag="hdfs://dc2/user/mrd/push/ltr/rank/"${one_day_ago}"/*/*/_SUCCESS"
hadoop fs -test -e ${user_short_hdfs_flag}
if [ $? -ne 0 ]
then
    echo ${user_short_hdfs}" file not exist"
    exit 2
fi
#get the news data from two weeks ago
array=()
for i in $(seq 1 14);
do
    temp=$(date -d "$1 - $i day")
    temp=$(date -d "$temp" +%Y%m%d)
    news_hdfs_one_day="hdfs://dc2/user/mrd/hive/warehouse/news_pool/t_news_index/hour=$temp*/*"
    array+=($news_hdfs_one_day)
done
news_hdfs_14_days=$(IFS=, ; echo "${array[*]}")
echo $news_hdfs_14_days

# spark params
MASTER=yarn-cluster
APP_NAME=ffm_prepare_watsonwei
QUEUE=root.media
NUM_EXECUTORS=200
EXECUTOR_MEMORY=10g
EXECUTOR_CORES=2
DRIVER_MEMORY=8g
DRIVER_CORES=2

spark-submit \
    --master ${MASTER} \
    --queue ${QUEUE} \
    --name ${APP_NAME} \
    --driver-memory ${DRIVER_MEMORY} \
    --driver-cores ${DRIVER_CORES} \
    --executor-cores ${EXECUTOR_CORES} \
    --num-executors ${NUM_EXECUTORS}  \
    --executor-memory ${EXECUTOR_MEMORY} \
    ./prepare_data.py ${push_event_hdfs} ${user_long_hdfs} ${news_hdfs_14_days} ${user_short_hdfs} ${time_hour}
