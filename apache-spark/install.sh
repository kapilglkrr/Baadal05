#!/bin/sh
 
# installation of Oracle Java JDK.
sudo apt-get -y update
sudo apt-get -y install python-software-properties
sudo add-apt-repository -y ppa:webupd8team/java
sudo apt-get -y update
sudo apt-get -y install oracle-java7-installer

# Installation of commonly used python scipy tools
sudo apt-get -y install python-numpy python-scipy python-matplotlib ipython ipython-notebook python-pandas python-sympy python-nose

# Installation of scala
wget http://www.scala-lang.org/files/archive/scala-2.11.7.deb
sudo dpkg -i scala-2.11.7.deb
sudo apt-get -y update
sudo apt-get -y install scala

# Downloading spark
wget http://d3kbcqa49mib13.cloudfront.net/spark-2.0.0-bin-hadoop2.7.tgz
tar -zxf spark-2.0.0-bin-hadoop2.7.tgz
sudo mv spark-2.0.0-bin-hadoop2.7 /usr/local/spark

#Additional packages required
sudo apt-get -y install python-pip
sudo apt-get -y install mongodb-server
sudo pip install nltk
sudo pip install pymongo

#Settingup path
echo "export PATH=$PATH:/opt/scala/bin" >> ~/.bashrc
echo "export PATH=$PATH:/usr/local/spark/bin" >> ~/.bashrc
echo "export PYTHONPATH=/usr/local/spark/python/:$PYTHONPATH" >> ~/.bashrc
echo "export PYTHONPATH=$PYTHONPATH:/usr/local/spark/python/lib/py4j-0.10.1-src.zip" >> ~/.bashrc
source ~/.bashrc


