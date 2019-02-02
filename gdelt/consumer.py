import time
import zmq
import random


context = zmq.Context()
# recieve work
consumer_receiver = context.socket(zmq.PULL)
consumer_receiver.connect("tcp://localhost:5557")

while True:
        work = consumer_receiver.recv_json()
        data = work['num']
        print work
