from cassandra.cluster import Cluster
import time
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from dateutil import parser
import numpy as np

cluster = Cluster()
session = cluster.connect()

fig, ax1 = plt.subplots()
#fig = plt.figure()
#ax1 = fig.add_subplot(1,1,1)
#fig, ax1 = plt.subplots()



def animate(i):
    print(" Check for updated data")
    rows = session.execute('SELECT * FROM tweetstock_space.batch_avg')
    rows = list(rows)
    print(len(rows))
    x_axis = np.zeros(len(rows), dtype=object)
    stock_y_axis = np.zeros(len(rows), dtype = object)
    twitter_y_axis = np.zeros(len(rows), dtype = object)
    rows.sort(key=lambda r: str(r[0]).split(" ")[1])
    for i,user_row in enumerate(rows):
        x_axis[i] = user_row[0]
        twitter_y_axis[i] = user_row[1][2]
        stock_y_axis[i] = user_row[1][1]
    print(" Graph updated")
    print(x_axis)
    print(stock_y_axis)
    print(twitter_y_axis)

    ax1.clear()

    ax1.set_xlabel('Time (DD HH:MM)')
    ax1.set_ylabel('Stock Price (USD)', color='b')
    ax1.plot(x_axis, stock_y_axis, 'b-')
    ax1.tick_params('y', colors='b')
    ax2 = ax1.twinx()
    ax2.clear()
    ax2.plot(x_axis, twitter_y_axis, 'r--')
    ax2.set_ylabel('Semantics (0-4)', color='r')
    ax2.set_ylim([0, 4])
    ax2.tick_params('y', colors='r')
    fig.autofmt_xdate()
    fig.tight_layout()



ani = animation.FuncAnimation(fig, animate, interval=6000)
plt.show()
#session.execute('DROP TABLE IF EXISTS twitterStock_space.avg')
