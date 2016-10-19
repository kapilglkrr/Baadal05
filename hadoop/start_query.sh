cat query.txt | while read LINE
do
	echo $LINE
	start_time=`date +%s`
	hadoop jar source_code/target/inverted-index-1.0-SNAPSHOT-jar-with-dependencies.jar cloud.Retrieval inverted-index-output input $LINE
	end_time=`date +%s`
	echo `expr $end_time - $start_time`>> time.txt
done