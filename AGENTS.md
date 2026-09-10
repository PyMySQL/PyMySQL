Do `ruff format` and `ruff check` before commit.

## Running the MySQL test suite in this container

1. Install the server and test dependencies if they are not already available:

   ```sh
   apt-get update
   DEBIAN_FRONTEND=noninteractive apt-get install -y mysql-server
   python -m pip install --upgrade -r requirements-dev.txt
   ```

2. Keep MySQL running in a separate, long-lived terminal/session. The container
   does not run systemd, so start `mysqld` directly instead of using
   `systemctl`:

   ```sh
   install -d -o mysql -g mysql /run/mysqld
   rm -f /run/mysqld/mysqld.sock /run/mysqld/mysqld.sock.lock \
     /run/mysqld/mysqlx.sock /run/mysqld/mysqlx.sock.lock \
     /run/mysqld/mysqld.pid
   mysqld --user=mysql --local-infile=1 \
     --socket=/run/mysqld/mysqld.sock \
     --pid-file=/run/mysqld/mysqld.pid
   ```

3. In another terminal/session, wait for MySQL and initialize a fresh data
   directory. The SQL files create databases and users, so only run them when
   `test1` does not exist:

   ```sh
   until mysqladmin ping --silent; do sleep 1; done
   if ! mysql -NBe "SHOW DATABASES LIKE 'test1'" | rg -q '^test1$'; then
     mysql -uroot --comments < ci/docker-entrypoint-initdb.d/init.sql
     mysql -uroot --comments < ci/docker-entrypoint-initdb.d/mysql.sql
     mysql -uroot --comments < ci/docker-entrypoint-initdb.d/mariadb.sql
   fi
   cp ci/docker.json pymysql/tests/databases.json
   ```

4. Run the same test commands as CI:

   ```sh
   pytest -v --cov --cov-config .coveragerc pymysql
   pytest -v --cov-append --cov-config .coveragerc \
     --doctest-modules pymysql/converters.py
   ```
