import overpy
import psycopg2
from functions import insertRow
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import time
from time import gmtime, strptime

def main(nodes,cursor,al,inserted):

    return(al,inserted)

def disp(x,y,X,Y):
    k = []
    for i in range(Y):
        s = []
        for j in range(X):
            if j == x and i == y:
                s.append('@')
            else:
                s.append('.')
    
        k.append(s)
    for i in k:
        print(*i)
al = 0
inserted=0
LATc = 4
LONc=4
LAT = 59.9454597
LON= 30.3271546
start_time = time.time()
connection = psycopg2.connect(
user="postgres",
password="postgres",
host="192.168.0.105",
port="5432",
database = "geojson")
connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = connection.cursor()
hj = (al,inserted)
for i in range(LATc):
    for j in range(LONc):
        rq3 = """[out:json];
    node({},{},{},{})(if:count_tags() > 0);
    /*added by auto repair*/
    (._;>;);
    /*end of auto repair*/
    out;""".format(LAT,LON,LAT+0.01,LON+0.01)
        
        
        for k in range(1,21):
            try:
                api = overpy.Overpass()
                r = api.query(rq3)
                
                print("Запрос выполнен",LAT,LON)
                break
            except:
                print('Попытка запроса ',k)
        nodes = r.nodes
        for node in nodes:
            insertRow(node,cursor)
        
        LON = LON+0.01
        disp(j,i,LONc,LATc)
    LAT = LAT + 0.01
        











endtime = (time.time() - start_time)
print('All =',al,'inserted =',inserted,'time =',"{}:{}:{:.2f}".format(int(endtime//3600),int(endtime % 3600 // 60),endtime % 60))





'''
"""
nwr[amenity](around:1000,59.9454597,30.3271546);
out;
"""
rq1 = """
nwr[amenity](around:3000,51.766654, 55.102026);
out;
"""
rq2 = """(nwr[amenity](around:300,51.766654, 55.102026);
 nwr[leisure](around:300,51.766654, 55.102026););
out;"""
rq3 = """[out:json];
node[leisure](around:700,, 55.102026);
/*added by auto repair*/
(._;>;);
/*end of auto repair*/
out;"""
'''
