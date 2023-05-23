create database test1 DEFAULT CHARACTER SET utf8mb4;
create database test2 DEFAULT CHARACTER SET utf8mb4;
create user test2           identified by 'some password';
grant all on test2.* to test2;
create user test2@localhost identified by 'some password';
grant all on test2.* to test2@localhost;

