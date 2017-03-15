# -*- coding: utf-8 -*-
"""
Created on Tue Mar 14 16:08:10 2017
@author: Thautwarm

"""
try:
    import MySQLdb
except:
    import pymysql as MySQLdb
from pymysql.cursors import DictCursor


getDataBase = lambda : MySQLdb.connect(host="x.x.x.x" , user="xxx" , passwd="xxx",db='xxx')

class deploy:
    """
    you can easily create a table associated with an entity in this way:
        (if I write a class named "user" in "config.json")
        
        from pymysql.easyMVC.dao import deploy
        from pymysql.easyMVC.entity import entities
        user=entities.user
        deploy(user).createTable()
        # now the table named user is established.
    
    also you can destroy a table named "user" in this way:
        from pymysql.easyMVC.dao import deploy
        from pymysql.easyMVC.entity import entities
        user=entities.user
        deploy(user).dropTable()
    
    and you can create all the tables associated with "config.json" in this way:
        from pymysql.easyMVC import entity,deploy
        entities=entity.entities
        for entity_i in entities.attrs:
            dao.deploy(entities.get(entity_i)).createTable()

    """
    def __init__(self,entity):
        self.entity=entity
    def createTable(self):
        entity=self.entity
        sql="create table if not exists %s(%s);"
        typemap=entity.typemap
        attrs=''.join("%s %s %s"%(key,typemap[key],'primary key auto_increment,' if key=='id' else ',') for key in typemap)
        db=getDataBase()
        with db:
            cur=db.cursor()
            cur.execute(sql%(entity.table,attrs[:-1]))
            cur.close()
        db.close()
    def dropTable(self):
        sql="drop table %s"%(self.entity.table)
        db=getDataBase()
        with db:
            cur=db.cursor()
            cur.execute(sql)
            cur.close()
        db.close()


#explain: the abstraction of some trivial SQL operations.
#explain: SQL操作抽象函数
def sendExecute(conn,sql,mode='void'):
    
    """
    execute SQL statements and return a corresponding reuslt decided by the param "mode".
    """
    with conn:
        cur=conn.cursor(DictCursor)
        if mode=='void':
            ret=cur.execute(sql)
            cur.close()
            return ret
        elif mode=='count':
            ret=cur.execute(sql)
            cur.close()
        elif mode=='df':
            cur.execute(sql)
            ret = cur.fetchall()
            cur.close()
        else:
            ret=None
            cur.close()
    return ret
    
def makeAttrArr(table,**maps):

        """
        each SQL operations will be done with passing the params like this way:
            userdao.delete(username='saber',password='123456')
        and this function is to dealing with the params to convert them into two lists, \
        first of which is a list of Keys, the other a list of Values (converted to string form) correspond with the Keys.

        for instance,
        function "makeAttrArr({'a':1,'b':2})" will return "(['a','b'],['1','2'])"
        """
        entity=maps.get(table)
        if entity:
            maps=entity.toMap()
        if not maps:
            return 
        
        keyarr=[]
        valuearr=[]
        for key in maps:
            keyarr.append(key)
            value=maps[key]
            valuearr.append( "'%s'"%value )
        return keyarr,valuearr
#end explain
    
    
class baseDao:
    def __init__(self,tablename):
        self.table=tablename
        self.conn=getDataBase()
    def add(self,**maps):
        """
        add record.
        增加记录（不判断重复）
        """
        attrarr=makeAttrArr(self.table,**maps)
        if not attrarr:return
        keyarr,valuearr=attrarr
        
        sql="insert into %s (%s) values (%s);"%(self.table,','.join(keyarr),','.join(valuearr))
        conn=self.conn
        return sendExecute(conn,sql)
        
    def delete(self,**maps):
        """
        delete records.
        删除记录（不判断重复）
        """
        attrarr=makeAttrArr(self.table,**maps)
        if not attrarr:return
        keyarr,valuearr=attrarr
        
        sql="delete from %s where %s;"%(self.table ,' and '.join('='.join(keypair) for keypair in zip(keyarr,valuearr)) )
        conn=self.conn
        return sendExecute(conn,sql)
        
    def check(self,**maps):
        
        """
        check how many records matched with the key-value pairs in the database. 
        """
        attrarr=makeAttrArr(self.table,**maps)
        if not attrarr:return
        keyarr,valuearr=attrarr
        
        sql="select id from %s where %s;"%(self.table ,' and '.join('='.join(keypair) for keypair in zip(keyarr,valuearr)) )
        conn=self.conn
        return sendExecute(conn,sql,mode='count')
        
    def select(self,**maps):
        """
        select the records matched with the key-value pairs in the database,
        which return a list of dict object like this way:

        print(userdao.select(username='archer'))
        >>>[{'id':5,'username':'archer','password':'enmiya','access':4,...},
            {'id':6,'username':'archer','password':'gilgamesh','access':1,...},
            ...
            ]
        """
        attrarr=makeAttrArr(self.table,**maps)
        if not attrarr:return
        keyarr,valuearr=attrarr
        
        conn=self.conn
        sql="select * from %s where %s;"%(self.table ,' and '.join('='.join(keypair) for keypair in zip(keyarr,valuearr)) )
        return sendExecute(conn,sql,mode='df')
        
    def selectAll(self):
        """
        return the results of SQL statements "SELECT * FROM %s;"%tableName
        """
        conn=self.conn
        sql="select * from %s"%self.table
        return sendExecute(conn,sql,mode='df')
    
    def change(self,**maps):
        """
        change the information of selected records.
        
        match the records by passing key-value pairs.
        and change the value of specific field by passing _key-value pairs, \
        like this way:
        
        userdao.change(username='caster',_username="rider")
        #you change usernames of all the records whose username are 'caster' to 'rider'. 
        """
        attrarr=makeAttrArr(self.table,**maps)
        if not attrarr:return

        keyarr=[]
        valuearr=[]
        tokeyarr=[]
        tovaluearr=[]
        for key,value in zip(*attrarr):
            if key[0]=='_':
                tokeyarr.append(key[1:])
                tovaluearr.append(value)
            else:
                keyarr.append(key)
                valuearr.append(value)
        conn=self.conn
        sql="UPDATE %s SET %s WHERE %s;"%(self.table ,','.join('='.join(tokeypair) for tokeypair in zip(tokeyarr,tovaluearr)),\
                                            ' and '.join('='.join(keypair) for keypair in zip(keyarr,valuearr))
                                          )
        return sendExecute(conn,sql)
        
        
    def __del__(self):
        self.conn.close()
        
            
                
            
        
        
        
        
        
        

        
        

        
        
        
        
             
        
        