# -*- coding: utf-8 -*-
"""
Created on Tue Mar 14 20:39:28 2017
@author: Thautwarm
service
"""
from . import entity,dao
baseDao=dao.baseDao
entities=entity.entities

def checkSafety(astr):
    if type(astr)!=str:
        return False
    return True
    

def userSignIn(username,password):
    userdao=baseDao('user')
#    username=user.username
#    password=user.password
    if not (checkSafety(username) and checkSafety(password)):return False #
    return userdao.check(username=username,password=password)
        
def userLogIn(user):
    maps=user.toMap()
    if len(maps)!=len(user.attrs):
        return "userLogin_incompleteInfo"
        
    userdao=baseDao('user')
    if  userdao.check(user=user):
        return "userLogin_duplicated"
    if userdao.add(user):
        return "userLogin_finished"
    return "userLogin--error"

def userChangeInfo(**maps):
    userdao=baseDao('user')
    if userdao.change(**maps):
        return "userChangeInfo--finished"
    else:
        return "userChangeInfo--error"
        


    
    
    
    
    
    
    
    
    
        
    
    
    
    
