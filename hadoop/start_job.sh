hdfs dfs -mkdir -p /user/hduser
hdfs dfs -put ./input /user/hduser
cd source_code
mvn clean compile assembly:single	
hadoop jar target/inverted-index-1.0-SNAPSHOT-jar-with-dependencies.jar cloud.BuildInvertedIndex  input inverted-index-output
sh start_query.sh

