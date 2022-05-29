#!/bin/bash

#########################################################################
# Help menu
#########################################################################
usage()
{
  echo "Generate social network messaging statistics and run SparkSQL app."
  echo
  echo "Syntax: ./start.sh [-h] [-n ENTRIES] [-g GROUPS] [-u USERS] [-c FILE_NAME] [-d DATABASE]"
  echo "Run without parameters is equivalent to:"
  echo "./start.sh -n 500 -g 8 -u 200 -c config.csv -d database_name"
  echo
  echo "Optional arguments:"
  echo "-h     help menu;"
  echo "-n     number of entries in statistics;"
  echo "-g     number of social network groups;"
  echo "-u     number of social network users;"
  echo "-c     config file;"
  echo "-d     database name."
}

#########################################################################
# Script arguments processing
#########################################################################

FILE_NAME="config.csv"
NUMBER_OF_ENTRIES=500
NUMBER_OF_GROUPS=8
DATABASE="database_name"
NUMBER_OF_USERS=200

function isDigit {
    re='^[0-9]+$'
    if ! [[ $2 =~ $re ]]
    then
        echo "Error: value of variable $1 is not a number" >&2
        exit 1
    elif [[ $2 -eq 0 ]]
    then
        echo "Error: Zero value of variable $1 is not permitted." >&2
        exit 1
    fi
}

while getopts ":n:g:u:c:d:h" option; do
    case $option in
        h)
           usage
           exit 0
           ;;
        n)
           NUMBER_OF_ENTRIES=${OPTARG}
           isDigit NUMBER_OF_ENTRIES $NUMBER_OF_ENTRIES
           ;;
        g)
           NUMBER_OF_GROUPS=${OPTARG}
           isDigit NUMBER_OF_GROUPS $NUMBER_OF_GROUPS
           ;;
        u)
           NUMBER_OF_USERS=${OPTARG}
           isDigit NUMBER_OF_USERS $NUMBER_OF_USERS
           ;;
        c)
           FILE_NAME=${OPTARG}
           ;;
        d)
           DATABASE=${OPTARG}
           ;;
       \?)
           echo "Error: Invalid option." >&2
           echo
           usage
           exit 1
           ;;
       *)
           echo "Error: Empty argument value." >&2
           echo
           usage
           exit 1
           ;;
    esac
done

#########################################################################
# Generate config with users and IDs of their groups
#########################################################################

export PATH=$PATH:/usr/local/hadoop/bin/
hadoop fs -rm -r statistics
hadoop fs -rm -r out
hadoop fs -rm -r input
hadoop fs -mkdir input

touch $FILE_NAME
echo "group,username" > $FILE_NAME

for i in $(seq 1 $NUMBER_OF_USERS)
    do
      username="user_$i"
      group="group_$[$RANDOM % $NUMBER_OF_GROUPS + 1]"
      echo "$group,$username" >> $FILE_NAME
    done

hadoop fs -put $FILE_NAME input
rm $FILE_NAME

#########################################################################
# Install PostgreSQL and generate database entries
# for messaging statistics in the following format:
# sender, receiver, date, message
#########################################################################

sudo apt-get update -y
sudo apt-get install -y postgresql postgresql-contrib
sudo service postgresql start

# Создаем таблицу с логами
sudo -u postgres psql -c 'ALTER USER postgres PASSWORD '\''1234'\'';'
sudo -u postgres psql -c 'drop database if exists '"$DATABASE"';'
sudo -u postgres psql -c 'create database '"$DATABASE"';'
sudo -u postgres -H -- psql -d $DATABASE -c 'CREATE TABLE statistics (id BIGSERIAL PRIMARY KEY, sender VARCHAR(10), receiver VARCHAR(10), date VARCHAR(30), message VARCHAR(256));'

# Генерируем входные данные и добавляем их в таблицу
DATE=2021-01-25
for i in $(seq 1 $NUMBER_OF_ENTRIES)
    do
        # generate random USERNAME:
        sender="user_$((RANDOM % $NUMBER_OF_USERS + 1))"
        receiver="user_$((RANDOM % $NUMBER_OF_USERS + 1))"
        date=$(date +%F -d "$DATE + $(($i / 10)) day")
        message_length=$((RANDOM % 25 + 1))
        message=$(shuf -zer -n$message_length {a..z} {A..Z} ' ' '.' '?' '!' {0..9})
        sudo -u postgres -H -- psql -d $DATABASE -c 'INSERT INTO statistics (sender, receiver, date, message) values ('\'"$sender"\'','\'"$receiver"\'','\'"$date"\'','\'"$message"''\'');'
    done

#########################################################################
# Install SQOOP, PostgreSQL driver, Spark
#########################################################################

if [ ! -f sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz ]; then
    wget http://archive.apache.org/dist/sqoop/1.4.7/sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz
    tar xvzf sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz
else
    echo "Sqoop already exists, skipping..."
fi

if [ ! -f postgresql-42.2.5.jar ]; then
    wget --no-check-certificate https://jdbc.postgresql.org/download/postgresql-42.2.5.jar
    cp postgresql-42.2.5.jar sqoop-1.4.7.bin__hadoop-2.6.0/lib/
else
    echo "Postgresql driver already exists, skipping..."
fi

export PATH=$PATH:/sqoop-1.4.7.bin__hadoop-2.6.0/bin

if [ ! -f spark-2.3.1-bin-hadoop2.7.tgz ]; then
    wget https://archive.apache.org/dist/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz
    tar xvzf spark-2.3.1-bin-hadoop2.7.tgz
else
    echo "Spark already exists, skipping..."
fi

export SPARK_HOME=/spark-2.3.1-bin-hadoop2.7
export HADOOP_CONF_DIR=$HADOOP_PREFIX/etc/hadoop

#########################################################################
# Run SparkSQL application and view the result
#########################################################################

sqoop import --connect 'jdbc:postgresql://127.0.0.1:5432/'"$DATABASE"'?ssl=false' --username 'postgres' --password '1234' --table 'statistics' --target-dir 'statistics'

export PATH=$PATH:/spark-2.3.1-bin-hadoop2.7/bin

spark-submit --class bdtc.lab2.SparkSQLApplication --master local --deploy-mode client --executor-memory 1g --name wordcount --conf "spark.app.id=SparkSQLApplication" /tmp/lab2-1.0-SNAPSHOT-jar-with-dependencies.jar user/root/statistics/ user/root/input/$FILE_NAME user/root/out

echo "DONE! RESULT IS: "
hadoop fs -cat  hdfs://127.0.0.1:9000/user/root/out/part-*




