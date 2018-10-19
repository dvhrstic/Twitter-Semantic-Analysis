from cassandra.cluster import Cluster
import time
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from dateutil import parser

cluster = Cluster()
session = cluster.connect()

fig = plt.figure()
ax1 = fig.add_subplot(1,1,1)

def animate(i):

    print(" Check for updated data")
    rows = session.execute('SELECT * FROM tweetstock_space.avg')
    x_axis = []
    y_axis = []
    for user_row in rows:
        x_axis.append(parser.parse(user_row[0]))
        y_axis.append(user_row[1][1])
    print(" Graph updated")
    print(user_row[0])
    print(x_axis)
    ax1.clear()
    ax1.plot(x_axis, y_axis, '-')

ani = animation.FuncAnimation(fig, animate, interval=60000)
plt.show()
#session.execute('DROP TABLE IF EXISTS twitterStock_space.avg')