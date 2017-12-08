#!/bin/bash -
#===============================================================================
#
#          FILE: ingestTwitterToLocalCluster.sh
#
#         USAGE: ./ingestTwitterToLocalCluster.sh
#
#   DESCRIPTION: Ingest the twitter data to AsterixDB
#
#       OPTIONS:
#  REQUIREMENTS: ---
#          BUGS: ---
#         NOTES: ---
#        AUTHOR: Jianfeng Jia (), jianfeng.jia@gmail.com
#  ORGANIZATION: ics.uci.edu
#       CREATED: 10/27/2015 11:06:01 AM PDT
#      REVISION:  ---
#===============================================================================

set -o nounset                              # Treat unset variables as an error

# ddl to register the twitter dataset
host=${1:-"http://localhost:19002/aql"}
nc=${2:-"blue"}
cat <<EOF | curl -XPOST --data-binary @- $host
create feed TweetFeed using socket_adapter
(
    ("sockets"="$nc:10021"),
    ("address-type"="nc"),
    ("type-name"="typeTweet"),
    ("format"="adm")
);
connect feed TweetFeed to dataset ds_tweet;
start feed TweetFeed;
EOF


#[ -f ./script/sample.adm.gz ] || { echo "Downloading the data...";  ./script/getSampleTweetsFromGDrive.sh; }

echo "Start ingestion ..." 
gunzip -c ./script/samplebr.adm.gz | ./script/fileFeed.sh $host 10021
echo "Ingested sample tweets."

