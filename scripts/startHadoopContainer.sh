c#!/usr/bin/env bash
if [[ $# -eq 0 ]] ; then
    echo 'You should specify container name!'
    exit 1
fi

echo "Creating .jar file..."
mvn package -f ../pom.xml

docker cp ../target/lab2-1.0-SNAPSHOT-jar-with-dependencies.jar $1:/tmp
docker cp start.sh $1:/

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

echo "Go to container with 'docker exec -it hadoop-psql bash' command and start script in it to:"
echo
usage