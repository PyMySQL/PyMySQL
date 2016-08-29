<!--- Provide a general summary of the issue in the Title above -->

<!---
IMPORTANT NOTE:
This project is maintained one busy person having frail wife and infant daughter.
My time and energy is very limited resource. I'm not teacher or free tech support.
Don't ask a question here.  Don't file an issue until you believe it's a not problem of your code.
Search friendly volunteer who can teach you or review your code on ML or Q&A site.

See also: https://medium.com/@methane/why-you-must-not-ask-questions-on-github-issues-51d741d83fde
--->


## Expected Behavior
<!--- If you're describing a bug, tell us what should happen -->
<!--- If you're suggesting a change/improvement, tell us how it should work -->

## Current Behavior
<!--- If describing a bug, tell us what happens instead of the expected behavior -->
<!--- If suggesting a change/improvement, explain the difference from current behavior -->

## Possible Solution
<!--- Not obligatory, but suggest a fix/reason for the bug, -->
<!--- or ideas how to implement the addition or change -->

## Executable script to reproduce (for bugs)

<!--- Overwrite following code and schema --->

code:
```python
import pymysql.cursors

# Connect to the database
connection = pymysql.connect(host='localhost',
                             user='user',
                             password='passwd',
                             db='db',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

try:
    with connection.cursor() as cursor:
        # Create a new record
        sql = "INSERT INTO `users` (`email`, `password`) VALUES (%s, %s)"
        cursor.execute(sql, ('webmaster@python.org', 'very-secret'))

    # connection is not autocommit by default. So you must commit to save
    # your changes.
    connection.commit()

    with connection.cursor() as cursor:
        # Read a single record
        sql = "SELECT `id`, `password` FROM `users` WHERE `email`=%s"
        cursor.execute(sql, ('webmaster@python.org',))
        result = cursor.fetchone()
        print(result)
finally:
    connection.close()
```

schema:
```sql
CREATE TABLE `users` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `email` varchar(255) COLLATE utf8_bin NOT NULL,
    `password` varchar(255) COLLATE utf8_bin NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
AUTO_INCREMENT=1 ;
```

## Tracebacks (for bugs)

```
paste here
```

## Context
<!--- How has this issue affected you? What are you trying to accomplish? -->
<!--- Providing context helps us come up with a solution that is most useful in the real world -->

## Your Environment
<!--- Include as many relevant details about the environment you experienced the bug in -->

* Operating System and version:
* Python version and build (cygwin, python.org, homebrew, pyenv, Linux distribution's package, PyPy etc...)
* PyMySQL Version used:
* my.cnf if possible.  If you don't have it, related system variables like [connection encoding](https://dev.mysql.com/doc/refman/5.6/en/charset-connection.html).
