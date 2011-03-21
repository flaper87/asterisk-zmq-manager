import zmq
import json

context = zmq.Context()

#  Socket to talk to server
print "Connecting to hello world server..."
socket = context.socket(zmq.REQ)
socket.connect ("tcp://localhost:967")

#  Do 10 requests, waiting each time for a response
for request in range (1,10):
    print "Sending request ", request,"..."
    data = dict(
            Command="Originate",
            Channel="SIP/400", 
            Context="house", 
            Exten="100",
            Account="123",
            CallerID="FlaPer87", 
            Priority="1",
            Variable={"MyVariable1" : "Value 1", "MyVariable2" : "Value 2"},
        )
    
    socket.send(json.dumps(data))
    
    # Get the reply.
    print "Received reply ", socket.recv()