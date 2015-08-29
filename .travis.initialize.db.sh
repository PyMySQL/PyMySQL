#!/bin/bash

#debug
set -x
#verbose
set -v

if [ ! -z "${DB}" ]; then
    # disable existing database server in case of accidential connection
    mysql -u root -e 'drop user travis@localhost; drop user root@localhost; drop user travis; create user super@localhost; grant all on *.* to super@localhost with grant option'
    mysql -u super -e 'drop user root'
    F=mysql-${DB}-linux-glibc2.5-x86_64
    mkdir -p ${HOME}/mysql
    P=${HOME}/mysql/${F} 
    if [ ! -d "${P}" ]; then
        wget http://cdn.mysql.com/Downloads/MySQL-${DB%.*}/${F}.tar.gz -O - | tar -zxf - --directory=${HOME}/mysql 
    fi
    if [ -f "${P}"/my.cnf ]; then
        O="--defaults-file=${P}/my.cnf" 
    fi
    if [ -x "${P}"/scripts/mysql_install_db ]; then
        I=${P}/scripts/mysql_install_db 
        O="--defaults-file=${P}/my.cnf" 
    else
        I=${P}/bin/mysqld
        IO=" --initialize " 
        O="--no-defaults " 
    fi
    ${I} ${O} ${IO} --basedir=${P} --datadir=${HOME}/db-"${DB}" --log-error=/tmp/mysql.err
    PWLINE=$(grep 'A temporary password is generated for root@localhost:' /tmp/mysql.err)
    PASSWD=${PWLINE##* }
    if [ -x ${P}/bin/mysql_ssl_rsa_setup ]; then
        ${P}/bin/mysql_ssl_rsa_setup --datadir=${HOME}/db-"${DB}"
    fi
     # sha256 password auth keys:
     openssl genrsa -out "${P}"/private_key.pem 2048
     openssl rsa -in "${P}"/private_key.pem -pubout -out "${P}"/public_key.pem
    ${P}/bin/mysqld_safe ${O} --ledir=/ --mysqld=${P}/bin/mysqld  --datadir=${HOME}/db-${DB} --socket=/tmp/mysql.sock --port 3307 --innodb-buffer-pool-size=200M  --lc-messages-dir=${P}/share --plugin-dir=${P}/lib/plugin/ --log-error=/tmp/mysql.err &
    while [ ! -S /tmp/mysql.sock  ]; do
       sleep 2
    done
    cat /tmp/mysql.err
    if [ ! -z "${PASSWD}" ]; then
        ${P}/bin/mysql -S /tmp/mysql.sock -u root -p"${PASSWD}" --connect-expired-password -e "SET PASSWORD = PASSWORD('')"
    fi
    mysql -S /tmp/mysql.sock -u root -e "create user ${USER}@localhost; create user ${USER}@'%'; grant all on *.* to  ${USER}@localhost WITH GRANT OPTION;grant all on *.* to  ${USER}@'%' WITH GRANT OPTION;"
    sed -e 's/3306/3307/g' -e 's:/var/run/mysqld/mysqld.sock:/tmp/mysql.sock:g' .travis.databases.json > pymysql/tests/databases.json
    echo -e "[client]\nsocket = /tmp/mysql.sock\n" > "${HOME}"/.my.cnf
else
    cp .travis.databases.json pymysql/tests/databases.json
fi
