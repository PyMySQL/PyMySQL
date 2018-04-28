#!/bin/bash

#debug
set -x
#verbose
set -v

if [ ! -z "${DB}" ]; then
    # disable existing database server in case of accidential connection
    sudo service mysql stop

    docker pull ${DB}
    docker run -it --name=mysqld -d -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -p 3306:3306 ${DB}
    sleep 10

    while :
    do
        sleep 5
        mysql -uroot -h 127.0.0.1 -P 3306 -e 'select version()'
        if [ $? = 0 ]; then
            break
        fi
        echo "server logs"
        docker logs --tail 5 mysqld
    done

    echo -e "[client]\nhost = 127.0.0.1\n" > "${HOME}"/.my.cnf

    mysql -e 'select VERSION()'
    mysql -uroot -e 'create database test1 DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;'
    mysql -uroot -e 'create database test2 DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;'

    mysql -u root -e "create user test2           identified by 'some password'; grant all on test2.* to test2;"
    mysql -u root -e "create user test2@localhost identified by 'some password'; grant all on test2.* to test2@localhost;"

    cp .travis/docker.json pymysql/tests/databases.json
else
    cat ~/.my.cnf

    mysql -e 'select VERSION()'
    mysql -e 'create database test1 DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;'
    mysql -e 'create database test2 DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;'

    mysql -u root -e "create user test2           identified by 'some password'; grant all on test2.* to test2;"
    mysql -u root -e "create user test2@localhost identified by 'some password'; grant all on test2.* to test2@localhost;"

    cp .travis/database.json pymysql/tests/databases.json
fi
