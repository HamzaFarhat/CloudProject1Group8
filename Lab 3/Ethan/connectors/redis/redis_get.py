import redis        # pip install redis
ip='34.124.124.251'
r = redis.Redis(host=ip, port=6379, db=0,password='SOFE4630U')
v=r.get('file1')
with open("./recieved.jpg", "wb") as f:
    f.write(v)
