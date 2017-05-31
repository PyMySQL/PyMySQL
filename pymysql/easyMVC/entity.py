# -*- coding: utf-8 -*-
"""
Created on Tue Mar 14 10:27:34 2017

@author: Thautwarm
"""
import json
with open('./config/config.json') as f:
    class_info=json.load(f)
with open('./config/config_dict.json') as f:
    class_type_map=json.load(f)
def giveType(keyname):
    for type_map_key in class_type_map:
        for sign in class_type_map[type_map_key]:
            if sign in keyname.lower() :
                return type_map_key
    return "varchar(255)"
class makeEntity:
    def __init__(self,init,name=None):
        if name:
            self.table=name
        if type(init) in [tuple,list]:  #定义一个Entity
            self.typemap=dict()
            for i in init:
                self.__setattr__(i,None)
                self.typemap[i]=giveType(i)
            self.attrs=set(init)
        elif type(init)==dict:   #集成所有Entity
            for i in init:
                self.__setattr__(i,init[i])
            self.attrs=set(init)
    def toMap(self):
        maps=dict()
        for attr in self():
            value=self[attr]
            if value:
                maps[attr]=value
        return maps
    def __call__(self):
        return self.attrs
    def get(self,key):
        return self.__getattribute__(key)
    def set(self,key,value):
        if key in self.attrs:
            self.__setattr__(key,value)
    def __getitem__(self,key):
        return self.__getattribute__(key)
    
entities=makeEntity(  dict( 
                     [ (config_i['class'], makeEntity(config_i['attrs'],name=config_i['class']))   for config_i in class_info ]
                     ) )
    
    
        
            
            