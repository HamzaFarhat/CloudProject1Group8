import os.path

import redis        # pip install redis

def createFolder():
    pathExists = os.path.exists('./out')
    if not pathExists:
        os.mkdir('./out')


ip='34.130.239.43'
createFolder()
r = redis.Redis(host=ip, port=6379, db=0,password='SOFE4630U')
keys = r.keys("*.tiff")
for key in keys:
    data = r.get(key)
    print("Getting " + key.decode())
    with open('./out/' + key.decode(), "wb") as file:
        file.write(data)
