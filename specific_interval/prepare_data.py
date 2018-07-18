"""
Yi Wei
"""
from pyspark import SparkContext
import json
import time
import os
import sys
def parse_feature(feature_fields, key, feature_num_limit=0, feature_rt=False):
    # feature
    if key=="cat":
        feature_idx=0
    elif key=="tag":
        feature_idx=1
    elif key=="topic":
        feature_idx=2
    elif key=="cat_news":
        feature_idx=3
    elif key=="tag_news":
        feature_idx=4
    elif key=="topic_news":
        feature_idx=5
    feature_list = []
    feature_set = set()
    length=len(feature_fields)
    if feature_idx in [0,1,2]:
        feature_fields=feature_fields[:min(int(len(feature_fields)*0.6),60)]
    elif feature_idx==4:
        feature_fields=feature_fields[:min(len(feature_fields),3)]
    if feature_fields:
        for n_u_c_fields in feature_fields:
            try:
                if len(n_u_c_fields) < 2:
                    continue
                if feature_num_limit != 0 and len(feature_list) == feature_num_limit:
                    break

                n_u_c_feature = int(n_u_c_fields["id"])
                n_u_c_score = float(n_u_c_fields["score"])
                if n_u_c_feature in feature_set:
                    continue
                if n_u_c_score<0.08 and feature_idx==5:
                    continue
                n_user_cats_feature = "%d:%d:%.4f" % (feature_idx, n_u_c_feature, n_u_c_score)
                feature_list.append(n_user_cats_feature)
                feature_set.add(n_u_c_feature)
            except Exception as e:
                print "parse feature error. ", e
                continue
        if feature_rt and len(feature_fields) > feature_num_limit:
            n_user_cats_fields_reverse = feature_fields
            if len(feature_fields) > feature_num_limit * 2:
                n_user_cats_fields_reverse = feature_fields.reverse()
            for n_u_c in n_user_cats_fields_reverse:
                try:
                    n_u_c_fields = n_u_c.strip().split(":")
                    if len(n_u_c_fields) < 2:
                        continue
                    if feature_num_limit != 0 and len(feature_list) == feature_num_limit * 2:
                        break

                    n_u_c_feature = int(n_u_c_fields[0])
                    n_u_c_score = float(n_u_c_fields[1])
                    if n_u_c_feature in feature_set:
                        continue
                    n_user_cats_feature = "%d:%d:%.4f" % (feature_idx, n_u_c_feature, n_u_c_score)
                    feature_list.append(n_user_cats_feature)
                    feature_set.add(n_u_c_feature)
                except Exception as e:
                    print "parse feature error. ", e
                    continue

    if len(feature_list) > 0:
        return " ".join(feature_list)
    else:
        return ""
# directory of push_event data in hdfs
push_event_hdfs=sys.argv[1]
# directory of user long-term profile data in hdfs
user_long_hdfs=sys.argv[2]
# directory of news profile data in hdfs
new_profile_hdfs=sys.argv[3]
# directory of user short-term profile data in hdfs
rank_hdfs=sys.argv[4]
spark_session = SparkContext()
pushEventData=spark_session.textFile(push_event_hdfs)\
    .filter(lambda line: line is not None)\
    .map(lambda line: line.strip().split("\t"))\
    .filter(lambda l:len(l)==4)\
    .map(lambda (cid,flag,oid,dateTime): (oid,(cid,"1",dateTime)) if flag=="pv" else (oid,(cid,"0",dateTime)))
def is_json(myjson):
  try:
    json_object = json.loads(myjson)
  except ValueError, e:
    return False
  return True
newsProfileData= spark_session.textFile(new_profile_hdfs)\
    .filter(lambda line:line is not None)\
    .map(lambda line:line.strip().split("\t"))\
    .filter(lambda line:is_json(line[8]))\
    .map(lambda line: json.loads(line[8]))\
    .filter(lambda dic:"oid" in dic.keys() and "catScores" in dic.keys() and "tag" in dic.keys() and "topic" in dic.keys())\
    .map(lambda line: (line["oid"],(parse_feature(line["catScores"],"cat_news"),parse_feature(line["tag"],"tag_news"),parse_feature(line["topic"],"topic_news"))))\
    .reduceByKey(lambda x,y: x)
userProfileDataL=spark_session.textFile(user_long_hdfs)\
    .filter(lambda line:line is not None)\
    .map(lambda line:line.strip().split("\t"))\
    .filter(lambda l:l[1]=="tag" or l[1]=="topic" or l[1]=="cat")\
    .map(lambda l:(l[0],(l[1],l[2])))\
    .map(lambda l:(l[0],(parse_feature(json.loads(l[1][1]),l[1][0]))))\
    .groupByKey()\
    .filter(lambda l:len(list(l[1]))==3)\
    .map(lambda l:(l[0],(list(l[1])[0],list(l[1])[1],list(l[1])[2])))
userProfileDataS=spark_session.textFile(rank_hdfs)\
    .filter(lambda line:line is not None)\
    .map(lambda line:line.strip().split("\t"))\
    .filter(lambda l: len(l)==13)\
    .map(lambda l:(l[1],(l[0],l[-3],l[-2],l[-1])))\
    .groupByKey()\
    .map(lambda l:(l[0],sorted(list(l[1]))[-1]))\
    .map(lambda (cid,(dateTime,cat_us,tag_us,topic_us)):(cid,(" ".join(cat_us.split(";"))," ".join(tag_us.split(";"))," ".join(topic_us.split(";")))))
result=pushEventData.join(newsProfileData)\
    .map(lambda (oid,((cid,flag,dateTime),(cat,tag,topic))):(cid,(oid,flag,dateTime,cat,tag,topic)))\
    .leftOuterJoin(userProfileDataL)\
    .leftOuterJoin(userProfileDataS)\
    .filter(lambda l:l[1][0][1]!=None or l[1][1]!=None)\
    .map(lambda (cid,(((oid,flag,dateTime,cat_news,tag_news,topic_news),ul),us)):
                (dateTime,cid,oid,flag,ul[0],ul[1],ul[2],cat_news,tag_news,topic_news,us[0],us[1],us[2])
         if ul!=None and us!=None else((dateTime,cid,oid,flag,ul[0],ul[1],ul[2],cat_news,tag_news,topic_news) if us==None else
                (dateTime, cid, oid, flag,cat_news,tag_news,topic_news,us[0],us[1],us[2])) )\
    .map(lambda tuple:" ".join(tuple))
os.system("hdfs dfs -rm -r hdfs://dc2/user/mrd/push/watson/"+sys.argv[5])
result = result.coalesce(100).saveAsTextFile("hdfs://dc2/user/mrd/push/watson/"+sys.argv[5])
