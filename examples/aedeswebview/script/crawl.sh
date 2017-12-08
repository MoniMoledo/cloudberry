set -o nounset                              # Treat unset variables as an error


export JAVA_HOME=/usr/lib/jvm/jdk1.8.0_121 #(verificar versao do java da maquina)
export PATH=$JAVA_HOME/bin:$PATH

~/asterix/opt/local/bin/start-sample-cluster.sh 

# ddl to star feed ingestion
host=${1:-"http://localhost:19002/aql"}
nc=${2:-"blue"}
cat <<EOF | curl -XPOST --data-binary @- $host
start feed twitter.ZikaTweetFeed;
start feed webhose.PostFeed;
EOF

echo "Start webhose crawler ..." 

cd ~/webcrawler
sbt "run-main Crawler \
-tk "1e8f7935-bacd-4c4e-a055-f1e52f8bbadd" \
-kw "dengue", "denguefever", "aedesaegypti", "zika", "zikavirus", "microcephaly", "microcefalia", "febreamarela", "chikungunya" \
-co "BR" \
-ds 1 \
-tglurl "http://localhost:9000/location" \
-u 127.0.0.1 \
-p 10010 \
-w 0 \
-b 50"

echo "Start twitter crawler ..." 

cd ~/cloudberry
sbt "project noah" "run-main edu.uci.ics.cloudberry.noah.feed.TwitterFeedStreamDriver \
-ck qy2ThJZg2UbLHxVmuelHtneBT \
-cs lSV8IK5cX0TBCRHwzILfxb36WiR98T2cNIR6fHK7EupjLmOuQS \
-tk 115418371-d7hqanVeHwdP8BC0uFOIw5WFdo8zOdMkn8kGqfFb \
-ts Yi7tOUI1TKH6wRKBJvE4HiJaHyVUVLHGVaIl4WE2FRbLJ \
-dv twitter \
-uds ds_dengue \
-zds ds_dengue \
-tr dengue denguefever aedesaegypti zika zikavirus microcephaly microcefalia febreamarela chikungunya\
-u 127.0.0.1 \
-p 10001 \
-w 0 \
-b 50"


