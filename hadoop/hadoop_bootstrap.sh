echo 'Starting Hadoop for Baadal05'
stop-all.sh
echo 'Removing temp files'
rm /home/hduser/mydata/data/* -r
rm /home/hduser/mydata/name/* -r
rm /home/hduser/mydata/tmp/* -r
echo 'Files Removed'
hadoop namenode -format
start-all.sh
echo 'Hadoop started successfully'
echo '--------------------------'
echo 'Process Running :\n'
jps

