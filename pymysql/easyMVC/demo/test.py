from pymysql.easyMVC import entity,dao
entities=entity.entities
for entity_i in entities.attrs:
   dao.deploy(entities.get(entity_i)).createTable()
u=dao.baseDao('user')
user=entities.user
user.username='functionpro'
user.password='123'
u.add(user=user)
u.add(username='saber',password='123')
print(u.change(username='lambda',_username='saber'))
if not u.check(username='archer'):
   u.add(username='archer',password='emiya')
else:
   u.change(username='archer',_username='lambda')
print(u.add(user=user))
print(userSignIn(username='archer',password='358'))
    
