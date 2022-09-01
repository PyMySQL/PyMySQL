# PyMySQLsv

This project contains classes based on the PyMySQL database client which have
been accelerated using a C extension. Why would you want to do such a thing? Isn't
the whole point of PyMySQL to be a pure Python database client? Well, yes.
However, the only C-based alternatives either have restrictive licenses, have
dependencies that a system administrator must install, or both.
A MySQL client that is fast, easy to install for a non-admin user, and has a
permissive license is something that hasn't existed.

## What does the 'sv' stand for?

This tag  borrowed from the Lamborghini Aventador SuperVeloce. SuperVeloce is 
Italian for "super velocity".

## What about CyMySQL?

CyMySQL is a great idea, but doesn't give the performance we were looking for.
It increases the perforance of PyMySQL about 10-15%, which still leaves it at
the second slowest client. It is also based on a PyMySQL codebase from years ago,
so it does not contain any recent bug fixes or features of that project.

## How fast can it be?

If it's still mostly based on PyMySQL, how fast can it really be? Here's one
benchmark. We uploaded
[this data file](http://studiotutorials.s3.amazonaws.com/eCommerce/2019-Dec.csv)
into a SingleStoreDB table six times to get a total of around 21 million rows of
data including one datetime column, one float column, and 8 character columns.
We then used PyMySQL, MySQLdb, and PyMySQLsv to fetch the entire table with `fetchone`,
`fetchmany(20)`, and `fetchall` using both buffered and unbuffered cursors. 
Here are the results.

|                          | PyMySQL | MySQLdb | PyMySQLsv |
|--------------------------|---------|---------|-----------|
| Buffered fetchone        | 224.8s  | 50.6s   | 19.9s     |
| Buffered fetchmany(20)   | 217.63s | 50.3s   | 15.5s     |
| Buffered fetchall        | 217.9s  | 49.6s   | 14.8s     |
| Unbuffered fetchone      | 230.5s  | 48.3s   | 25.3s     |
| Unbuffered fetchmany(20) | 224.0s  | 35.0s   | 14.6s     |
| Unbuffered fetchall      | 232.4s  | 37.7s   | 29.2s     |

As you can see the gains are quite significant for this test case. Even MySQLdb,
which is based on the MySQL libraries takes twice as long in all but one
of the categories.

## Install

This package installs just like any other Python package. Since it includes a C
extension it does require a C compiler if there isn't a pre-compiled version for your
architecture.
```
python3 setup.py install
```

## How to use it

This package contains a Python DB-API compliant interface. So connections are made
the same way as any other DB-API connection.

```
import pymysql as sv

with sv.connect(...) as conn:
    with conn.cursor() as cur:
       ...
```

## License

This library is licensed under the [Apache 2.0 License](https://raw.githubusercontent.com/singlestore-labs/PyMySQLsv/main/LICENSE?token=GHSAT0AAAAAABMGV6QPNR6N23BVICDYK5LAYTVK5EA).

## Resources

* [PyMySQL](https://pymysql.readthedocs.io/en/latest/)
* [SingleStore](https://singlestore.com)
* [Python](https://python.org)

## User agreement

SINGLESTORE, INC. ("SINGLESTORE") AGREES TO GRANT YOU AND YOUR COMPANY ACCESS TO THIS OPEN SOURCE SOFTWARE CONNECTOR ONLY IF (A) YOU AND YOUR COMPANY REPRESENT AND WARRANT THAT YOU, ON BEHALF OF YOUR COMPANY, HAVE THE AUTHORITY TO LEGALLY BIND YOUR COMPANY AND (B) YOU, ON BEHALF OF YOUR COMPANY ACCEPT AND AGREE TO BE BOUND BY ALL OF THE OPEN SOURCE TERMS AND CONDITIONS APPLICABLE TO THIS OPEN SOURCE CONNECTOR AS SET FORTH BELOW (THIS “AGREEMENT”), WHICH SHALL BE DEFINITIVELY EVIDENCED BY ANY ONE OF THE FOLLOWING MEANS: YOU, ON BEHALF OF YOUR COMPANY, CLICKING THE “DOWNLOAD, “ACCEPTANCE” OR “CONTINUE” BUTTON, AS APPLICABLE OR COMPANY’S INSTALLATION, ACCESS OR USE OF THE OPEN SOURCE CONNECTOR AND SHALL BE EFFECTIVE ON THE EARLIER OF THE DATE ON WHICH THE DOWNLOAD, ACCESS, COPY OR INSTALL OF THE CONNECTOR OR USE ANY SERVICES (INCLUDING ANY UPDATES OR UPGRADES) PROVIDED BY SINGLESTORE.
BETA SOFTWARE CONNECTOR

Customer Understands and agrees that it is  being granted access to pre-release or “beta” versions of SingleStore’s open source software connector (“Beta Software Connector”) for the limited purposes of non-production testing and evaluation of such Beta Software Connector. Customer acknowledges that SingleStore shall have no obligation to release a generally available version of such Beta Software Connector or to provide support or warranty for such versions of the Beta Software Connector  for any production or non-evaluation use.

NOTWITHSTANDING ANYTHING TO THE CONTRARY IN ANY DOCUMENTATION,  AGREEMENT OR IN ANY ORDER DOCUMENT, SINGLESTORE WILL HAVE NO WARRANTY, INDEMNITY, SUPPORT, OR SERVICE LEVEL, OBLIGATIONS WITH
RESPECT TO THIS BETA SOFTWARE CONNECTOR (INCLUDING TOOLS AND UTILITIES).

APPLICABLE OPEN SOURCE LICENSE: Apache 2.0

IF YOU OR YOUR COMPANY DO NOT AGREE TO THESE TERMS AND CONDITIONS, DO NOT CHECK THE ACCEPTANCE BOX, AND DO NOT DOWNLOAD, ACCESS, COPY, INSTALL OR USE THE SOFTWARE OR THE SERVICES.
