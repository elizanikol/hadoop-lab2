# hadoop-lab2

## Task
1. Бизнес-логика:
Программа должна подсчитывать среднее количество сообщений пользователей групп социальной сети, передаваемых друг-другу. 
Входные данные: справочник группа-пользователь, записи пользователь1-пользователь2-дата-текст. 
Выходные данные: группа-среднее количество сообщений на одного пользователя в группе.

2. Технология подачи новых данных в систему:
Sqoop importer (PostgreSQL to HDFS)

3. Технология хранения:
HDFS

4. Технология обработки данных:
Spark SQL (DataFrame, DataSet)

### Cхема взаимодействия компонентов
![scheme](https://user-images.githubusercontent.com/55412039/170956315-9051937e-d321-4270-a244-1415b7b09c1e.png)

## Результаты прогона тестов
Тестовые данные и ожидаемые результаты хранятся в директории `src/test/testing-values/`.
<img width="474" alt="Screenshot 2022-05-30 at 12 01 26" src="https://user-images.githubusercontent.com/55412039/170958135-50052430-2d8a-4991-af89-516dc9eeda38.png">

## HDFS
Скриншоты файлов, загруженных в HDFS (первые 20 строк):
1. Файл со статистикой по переданным сообщениям (`hadoop fs -cat  hdfs://127.0.0.1:9000/user/root/statistics/part-*`):
<img width="516" alt="Screenshot 2022-05-30 at 12 31 54" src="https://user-images.githubusercontent.com/55412039/170963345-b8614a21-05c7-4f34-bd58-c7dd5fd9386e.png">
В БД PostgreSQL данные загружаются в таблицу statistics с полями id, sender, receiver, date, message.

2. Файл-справочник соответствия пользователей группам социальной сети (`hadoop fs -cat  hdfs://127.0.0.1:9000/user/root/input/config.csv`):
<img width="148" alt="Screenshot 2022-05-30 at 12 32 17" src="https://user-images.githubusercontent.com/55412039/170963578-18edbce5-2f48-489f-bc14-4bcecee4d470.png">

## Sqoop

Для загрузки данных в HDFS использовался Sqoop.
Результат загрузки 500 строк из БД:
<img width="950" alt="Screenshot 2022-05-29 at 23 35 43" src="https://user-images.githubusercontent.com/55412039/170965580-f87f58b7-d42e-4eab-90f0-c76244a2bf17.png">

## SparkSQLApplication

Результат работы приложения - статистика по группам и среднему количеству переданных сообщений пользователями этих групп:
<img width="218" alt="Screenshot 2022-05-30 at 02 23 31" src="https://user-images.githubusercontent.com/55412039/170965821-04f28d25-52d5-4d75-9077-3db237c9a552.png">

## Сборка и запуск

На хосте должен быть установлен maven
    
    cd /opt
    wget https://www-eu.apache.org/dist/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz
    tar xzf apache-maven-3.6.3-bin.tar.gz
    ln -s apache-maven-3.6.3 maven
    vi /etc/profile.d/maven.sh
    С содержимым
    export M2_HOME=/opt/maven
    export PATH=${M2_HOME}/bin:${PATH}


` docker pull zoltannz/hadoop-ubuntu:2.8.1`

Запуск docker-контейнера:

` docker run --name hadoop-psql -p 2122:2122 -p 8020:8020 -p 8030:8030 -p 8040:8040 -p 8042:8042 -p 8088:8088 -p 9000:9000 -p 10020:10020 -p 19888:19888 -p 49707:49707 -p 50010:50010 -p 50020:50020 -p 50070:50070 -p 50075:50075 -p 50090:50090 -t zoltannz/hadoop-ubuntu:2.8.1`

В новом терминале в папке scripts выполнить `./startHadoopContainer.sh hadoop-psql`.
При условии успешной сборки и прогона юнит-тестов в директории `target/` появится файл `lab2-1.0-SNAPSHOT-jar-with-dependencies.jar` – пакет со всеми зависимостями.

Запуск SparkSQL приложения с помощью скрипта /start.sh:
```
Generate social network messaging statistics and run SparkSQL app.

Syntax: ./start.sh [-h] [-n ENTRIES] [-g GROUPS] [-u USERS] [-c FILE_NAME] [-d DATABASE]
Run without parameters is equivalent to:
./start.sh -n 500 -g 8 -u 200 -c config.csv -d database_name

Optional arguments:
-h     help menu;
-n     number of entries in statistics;
-g     number of social network groups;
-u     number of social network users;
-c     config file;
-d     database name.

```

Опции предназначены для корректировки параметров скрипта генерации статистики по передаваемым соообщениям между поользователями.

Просмотр результатов работы программы выполняется в конце работы скрипта с помощью команды:
```bash
hadoop fs -cat  hdfs://127.0.0.1:9000/user/root/out/part-*
```
