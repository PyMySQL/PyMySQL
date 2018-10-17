import pymysql
import pymysql.cursors

def create_conn(host, user, passwd, db, charset='utf8mb4'):
    connection = pymysql.connect(host=host,
                             user=user,
                             password=passwd,
                             db=db,
                             charset=charset,
                             cursorclass=pymysql.cursors.DictCursor)
    return connection


'''
连接池是好和坏的。在并发数上来之后，如果连接池设置的太小，就会导致没有可用
连接，如果太大，又会导致其它可能需要连接的应用没有连接可用。设计的话，要根据
理想并发量来设置。比如说，如果，只考虑100人同时访问，那就设置100个连接，但是
如果连接一旦超过100个，第100之后的连接可能就会延迟非常高，网页会处于加载中，
但是在没人访问的时候，我希望连接能够释放资源这样也会更省电。

所以，最终的设计方案为：连接池不是一开始初始化的时候就创建N个连接，而是由用户
请求而创建，当达到连接池最大大小时，就停止创建连接；还有一个要求是，连接池必
须在达到100个之后，如果没有人访问，就应该关闭连接。

如何检测没有用户访问，用户访问一次，连接池里的计数一次，如果隔一段时间，这个
计数没有发生改变，就释放所有的连接，那么还必须启动一个线程去每隔5分钟去检测
自己的计数。但是这样就会造成安全问题，如果检测线程在关闭连接的时候，恰好有个
用户在使用数据库连接怎么办？

所以通过这种设计方式是错误的。

另一种设计思路是：一开始初始化线程池最大数，然后如果线程池里的连接全部被占用，
那么就创建新的连接来服务，服务完毕之后，再关闭掉。这种设计的好处是不会使连接池
不够用之后导致应用拒绝服务，坏处是，当没有用户访问的时候，数据库连接将处于空闲
状态。但是，利大于弊。

最终的方案应采用后者。

而且后者的优点很多，最后代码写完之后，这个设计的实现简直是精妙。
'''

from collections import deque

class MySQLConnPool:
    def __init__(self, host, user, passwd, db, size=100, charset='utf8mb4'):
        self.conns = deque()
        for i in range(size):
            conn = create_conn(host, user, passwd, db, charset)
            self.conns.append(conn)

        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db
        self.size = size
        self.used_conn_count = 0
        self.charset = charset

    def get_conn(self):
        '''
        当实际使用数，没有达到连接池限制，则pop
        否则，创建
        '''
        if self.used_conn_count < self.size:
            self.used_conn_count += 1
            return self.conns.pop()
        else:
            conn = create_conn(self.host, self.user, self.passwd, self.db, self.charset)
            return conn

    def back_conn(self, conn):
        '''
        当连接池里的连接被使用，才增加进去，如果连接池里的连接
        没被使用，增加进去就会超过连接限制，所以应该关闭。
        '''
        if self.used_conn_count > 0:
            self.used_conn_count -= 1
            self.conns.append(conn)
        else:
            conn.close()
