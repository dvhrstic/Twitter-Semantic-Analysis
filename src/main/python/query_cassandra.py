from cassandra.cluster import Cluster
import time

cluster = Cluster()
session = cluster.connect()

curr_time = "None"
while(True):
    print(" Check for updated data")
    rows = session.execute('SELECT * FROM tweetstock_space.avg')
    new_time = rows[0][0]
    if(new_time != curr_time):
        for user_row in rows:
            print(user_row)
        print(" Graph updated")
        curr_time = new_time
    print("Sleeping")
    time.sleep(60)
#session.execute('DROP TABLE IF EXISTS twitterStock_space.avg')