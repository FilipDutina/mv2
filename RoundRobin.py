import socket
import os
import sys
from _thread import *
from queue import Queue
import time
import threading
from threading import Event, Lock
import string
#from random import randint
from random import *
import psutil

ServerSocket = socket.socket()
host = '127.0.0.1'
port = 1233
ThreadCount = 0
totalMessagesSentCount = 0

#clients = []
#clients_lock = threading.Lock()

NUM_OF_MESSAGES = 395

random_flag = True
sticky_flag = False

flag = 1
available = True
endMessage = False
emptyQueues = 0
event = Event()
mutex = Lock()

#fix [Errno 98]
ServerSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

try:
    ServerSocket.bind((host, port))
except socket.error as e:
    print(str(e))
    
print('Waiting for a Connection..')
ServerSocket.listen(2)

def get_size(bytes, suffix="B"):
    """
    Scale bytes to its proper format
    e.g:
        1253656 => '1.20MB'
        1253656678 => '1.17GB'
    """
    factor = 1024
    for unit in ["", "K", "M", "G", "T", "P"]:
        if bytes < factor:
            return f"{bytes:.2f}{unit}{suffix}"
        bytes /= factor

cpufreq         = psutil.cpu_freq()
svmem           = psutil.virtual_memory()
"""print(f"Max Frequency: {cpufreq.max:.2f}Mhz")
print(f"Min Frequency: {cpufreq.min:.2f}Mhz")
print(f"Current Frequency: {cpufreq.current:.2f}Mhz")"""
#print(f"********************CPU percentage: {psutil.cpu_percent()}%")

print("CPU Usage Per Core:")
for i, percentage in enumerate(psutil.cpu_percent(percpu=True, interval=1)):
    print(f"Core {i}: {percentage}%")
pocetni_cpu = psutil.cpu_percent()
print(f"********************Total CPU Usage: {pocetni_cpu}%")

print(f"********************Ram percentage: {svmem.percent}%")

#pocetni_cpu = psutil.cpu_percent()
pocetni_ram = svmem.percent

class Publisher():
    def __init__(self, cname):
        #super(BrokerFirst, self).__init__(cname,**kwargs)
        self.name = cname
        self.cnt = 0
        self.end = False
        
    def generate_string(self, RecvBroker):
        global endMessage
        global NUM_OF_MESSAGES
        min_char = 5
        max_char = 8
        while self.cnt < NUM_OF_MESSAGES:
            allchar = string.ascii_letters + string.digits
            data = "".join(choice(allchar) for x in range(randint(min_char, max_char)))
            #ako je velicina src.q manja od broja klijenata vezanih za taj broker onda stavi 
            #poruku u src.q
            if RecvBroker.q.qsize() < RecvBroker.numberOfClients:
                RecvBroker.q.put(data)
            else:
                #ako je velicina src.q jednaka broju klijenata na brokeru, 
                #stavi poruke na src.q2
                RecvBroker.q2.put(data)
            self.cnt = self.cnt + 1
            #time.sleep(.1)
        endMessage = True
        self.end = True
        #print("VREME SLANJA JE {}".format(toc - tic))
        print("END MESSAGE SET TO TRUE")

class Broker():
    #constructor for first broker
    def __init__(self, name):
        #super(BrokerFirst, self).__init__(cname,**kwargs)
        self.q = Queue()
        self.q2 = Queue()
        self.numberOfClients = 0
        self.name = name
        self.visited = False
        self.neighbours = []   
        self.ThreadCount = 0
        self.messageCount = 0
        self.clientList = []
        self.clientNames = []
        
    def threaded_client(self):
        global emptyQueues
        global endMessage
        global totalMessagesSentCount
        global random_flag
        global sticky_flag
        #connection.send(str.encode('Welcome to the Server\n'))
        print("Broker {} salje poruku".format(self.name))
        print("--------------------VELICINA REDA U {} JE {}".format(self.name, self.q.qsize()))
        print("--------------------VELICINA REDA2 U {} JE {}".format(self.name, self.q2.qsize()))
        #event.set()
        
        if self.q.empty() == True and endMessage == True:
            print("{} broker has empty queue".format(self.name))
            emptyQueues += 1
            return

        print("Duzina liste {}".format(len(self.clientList)))

        #roundRobin
        if(False == random_flag and False == sticky_flag):
            print("\n ROUND ROBIN")

            for c in self.clientList:
                #data = input("Enter message for client: ")
                #print("GARISA")
                print("********** SALJEM PORUKU - RR **********")
                if self.q.empty() == True and self.q.empty() == True:
                    event.set()
                    return
                mutex.acquire()
                data = self.q.get()
                mutex.release()
                print("{} sending {}".format(self.name, data))
                c.sendall(str.encode(data))
                totalMessagesSentCount += 1
                """if totalMessagesSentCount == 100:
                    event.set()
                    return"""
                time.sleep(.1)
            event.set()
        #sticky
        elif(False == random_flag and True == sticky_flag):
            print("\n STICKY")
            #this number represents how many times certain client will be targeted
            #repeat_client = 0
            """
            for c in self.clientList:
                repeat_client = randint(1, 4)
                mutex.acquire()
                data = self.q.get()
                mutex.release()
                for iter in range(0, repeat_client):
                    print("{} sending ---> {}".format(self, data))
                    c.sendall(str.encode(data))
                    time.sleep(.1)
                    """
            #choosing random broker (sticky is in fact random algorith which targets same client multiple times)
            random_broker = randint(0, 2)
            if(1 == random_broker):
                length = len(self.clientList)
                #choosing random client
                random_client = randint(0, length - 1)

                for c in self.clientList:
                    if (c == self.clientList[random_client]):
                        while(True):
                            #data = input("Enter message for client: ")
                            print("********** SALJEM PORUKU - sticky **********")
                            if self.q.empty() == True and self.q.empty() == True:
                                event.set()
                                return
                                break
                            mutex.acquire()
                            data = self.q.get()
                            mutex.release()
                            print("{} sending {}".format(self.name, data))
                            c.sendall(str.encode(data))
                            totalMessagesSentCount += 1
                            """if totalMessagesSentCount == 100:
                                event.set()
                                return"""
                            time.sleep(.1)
                        event.set()
        #random
        elif(True == random_flag and False == sticky_flag):
            print("\n RANDOM")
            #choosing random broker
            random_broker = randint(0, 2)
            if(1 == random_broker):
                length = len(self.clientList)
                #choosing random client
                random_client = randint(0, length - 1)

                for c in self.clientList:
                    #data = input("Enter message for client: ")
                    print("********** SALJEM PORUKU - random **********")
                    if self.q.empty() == True and self.q.empty() == True:
                        event.set()
                        return
                    mutex.acquire()
                    data = self.q.get()
                    mutex.release()
                    print("{} sending {}".format(self.name, data))
                    c.sendall(str.encode(data))
                    totalMessagesSentCount += 1
                    """if totalMessagesSentCount == 100:
                        event.set()
                        return"""
                    time.sleep(.1)
                event.set()
        else:
            print("\n\n SOMETHING WENT WRONG! PLEASE CHECK YOUR GLOBAL ALGORITHM FLAGS! \n\n")
            sys.exit()
 
    def start_message_exchange(self):
        #send message from current broker to its neighbour broker
        print("POKRECEM START MESSAGE EXCHANGE ZA {}".format(self.name))
        t = threading.Thread(target=self.ReadFromQueueSendToQueue, args=[self, self.neighbours[0]])
        t.start()
        time.sleep(.1)
    
    """
    def start_queue_exchange(self):
        t = threading.Thread(target=self.read_from_broker_queue, args=[])
        t.start()
        time.sleep(.1)
    """
        
    def wait_for_conn(self):
        #global clients
        while self.ThreadCount < self.numberOfClients:
            Client, address = ServerSocket.accept()
            self.clientList += [Client]
            print('Connected to: ' + address[0] + ':' + str(address[1]))
            self.ThreadCount += 1
            print('Thread Number: ' + str(self.ThreadCount))
        #ServerSocket.close()
        
    def close_connection(self):
        for c in self.clientList:
            #c.sendall(str.encode("end"))
            c.close()
        
    #method for moving messages that could not be sent from 
    #current to next broker           
    def ReadFromQueueSendToQueue(self, src, dst):
        #print("\nReadFromQueueSendToQueue\n")
        global endMessage
        if src.q2.empty():
            print("src.q2 prazan")
        else:
            print("src.q2 nije prazan")
        while True:
            iter = 0
            event.wait()

            #dokle god src.q2 nije prazan
            while src.q2.empty() == False:

                #take message from queue and send it to next broker queue
                #uzme poslednju poruku iz src.q2
                a = src.q2.get()
                
                #ako je velicina dst.q manja od broja klijenata dst
                if dst.q.qsize() < dst.numberOfClients:

                    #prebaci poslednji element iz src.q2 na dst.q
                    dst.q.put(a)
                    break
                else:
                    #u slucaju da je velicina dst.q jednaka broju klijenata na tom brokeru,
                    #onda se element iz src.q2 prebacuje na dst.q2
                    #(koji ce u narednoj iteraciji biti prebacivan ili na q narednog brokera, ili na q2)
                    dst.q2.put(a)
                event.clear()
                #print("PREPISUJEM {} iz {} u {}".format(a, src.name, dst.name))
                iter += 1
                #dst.q2.put(a)
                #send.wait()
    
    """
    1. izracunati koliko krugova ce obici poruke u grafu
    2. pomnoziti broj krugova sa brojem klijenata svakog cvora
    3. ostaviti taj broj u q, a ostalo prebaciti u q2
    4. poruke koje se nalaze u q2 kruze kroz graf
    
    
    def read_from_broker_queue(self):
        global endMessage
        while True:
            #event.wait()
            while self.q.qsize() < self.numberOfClients and endMessage == False:
                print("GARIIIIIII MOOOOJ")
                data = self.q2.get()
                self.q.put(data)
            #event.clear()
    """
    
        
#graph       
class Graph:
    nodes = list()
    edges = list()
    cnt = 0

    #constructor
    def __init__(self):
        self.nodes = list()
        self.edges = list()

    #breadth first search algorithm
    def BFS(self, s):
        global flag
        print("\n B F S \n")
        #mark all the vertices as not visited
        for i in range(len(self.nodes)):
            self.nodes[i].visited = False
        
        #mark first node as visited - starting node
        s.visited = True

        #create a queue for BFS
        queue = list()

        #enqueue the first node
        queue.append(s)
        #do the message publishing for the first node
        if flag == 1:
            s.wait_for_conn()
            s.start_message_exchange()
            #s.start_queue_exchange()
            print("Sacekao sam konekcije")
        elif flag == 2:
            #for c in s.clientList:
            s.threaded_client()
            print("****************** flag == 2 moj dobri GARISONER {}".format(s.name))
        else:
            s.close_connection()
        #for c in clients:
        #    print("CHECK CLIENT {}".format(c))
        #    s.threaded_client(c)
        print("Poslao sam poruke")
        #s.messageCount = 0
        
        #go through the graph, until all the nodes are visited
        while len(queue) != 0:
            print("Usao u queue")
            #dequeue a vertex from queue and print it
            s = queue.pop()
                
            #get all the adjacent brokers of the dequeued broker s.
            #if adjacent broker has not been visited, mark it as visited
            #and enqueue it
            for broker in s.neighbours:
                print("Proveravam komsijske cvorove")
                if broker.visited == False:
                    print("Stavljam u red {}".format(broker.name))
                    queue.append(broker)
                    #publish messages for current broker
                    if flag == 1:
                        print()
                        print("Waiting for conn")
                        broker.wait_for_conn()
                        broker.start_message_exchange()
                        #broker.start_queue_exchange()
                    elif flag == 2:
                        print()
                        print("Sending messages")
                        #for c in broker.clientList:
                        broker.threaded_client()
                    else:
                        broker.close_connection()
                    broker.visited = True           
       
                        
                    
#testing graph - 3 brokers added
def Get_Predefined_Dag():
    sub_count = 1
    clientCount = 0
    nodes = int(input("Enter total number of nodes in graph: "))
    adjacency = list()

    print("\n")

    G = Graph()
    
    #make broker instances
    objs = []
    for i in range(nodes):
        print("GARI PRAVIM BROKERE")
        objs.append(Broker("Broker" + str(i)))
        print("GARI BROKER: {}".format(objs[i]))
        print("IME BROKERA: {}".format(objs[i].name))
    print("\n")    
    for i in range(nodes):
        print("STAMPAM IMENA: {}".format(objs[i].name))
    print("\n")
    for i in range(nodes):
        numberOfClients = int(input("Enter total number of clients for Broker{}: ".format(i+1)))
        objs[i].numberOfClients = numberOfClients
        print("BROKER {} IMA {} KLIJENATA: ".format(objs[i].name, objs[i].numberOfClients))
        for j in range(objs[i].numberOfClients):
            objs[i].clientNames.append("Client" + str(clientCount))
            clientCount += 1
            print("CLIENT COUNT {}".format(clientCount))
    print("\n")
    #add number of clients for each broker
    
    for i in range(nodes - 1):
        objs[i].neighbours.append(objs[i+1])
        print("objs[i].neighbours.append(objs[i+1]): %s\n" % objs[i+1].name)
    
    objs[nodes - 1].neighbours.append(objs[0])
    print("\n")
    for i in range(nodes):
        print("BROKER: {}".format(objs[i].name))
        G.nodes.append(objs[i])
    print("\n")
    #B1.wait_for_conn()
    
    #for c in clients:
    #    B1.threaded_client(c)
        
    print("NAPRAVIO SAM KLIJENTE")
    
    #testing queue
    """
    max_char = 12
    min_char = 8
    cnt = 0
    ukupnoPoruka = 0
    for i in range(nodes):
        cnt = 0
        while cnt < 10:
            ukupnoPoruka += 1
            allchar = string.ascii_letters + string.digits
            data = "".join(choice(allchar) for x in range(randint(min_char, max_char)))
            objs[i].q.put(data)
            cnt = cnt + 1
            time.sleep(.1)
        #objs[0].q.put("GArisa")
    print("IMAM OVOLIKO CNT {}".format(ukupnoPoruka))
    """
                
    
    objs[0].q.put("Message One")
    objs[0].q.put("Message Four")
    objs[0].q.put("Message Five")
    objs[0].q.put("Message Six")
    objs[0].q.put("Liman")
    
    # Append adjacencies
    for i in range(len(adjacency)):
        G.edges.append(adjacency[i])
        print("G.edges.append(adjacency[i]): %s\n", adjacency[i])
    # Append subscribers
    #G.subscribers = {G.nodes[0]: [S1, S2], G.nodes[1]: [S3, S4, S5], G.nodes[2]: [S6]}
    print("\n")
    return G
print("\n\n")
graph = Get_Predefined_Dag()
graph.BFS(graph.nodes[0])
print("\n")
P1 = Publisher("Publisher")
t1 = threading.Thread(target=P1.generate_string, args=[graph.nodes[0]])
tic = time.perf_counter()
t1.start()
print("Flag set")
print("DUZINA GRAFA {}".format(len(graph.nodes)))
flag = 2 
print("\n\n*********************** ispred while-a *********************\n")
"""
for c in graph.nodes[0].clientList:
    print("Node 0 {}".format(c))
print("Broker 2")
for c in graph.nodes[1].clientList:
    print("Node 1 {}".format(c))
"""

#time.sleep(10)

while True:
    graph.BFS(graph.nodes[0])
    if totalMessagesSentCount == (NUM_OF_MESSAGES + 5):
        print("USAO SAM NA KRAJ")
        toc = time.perf_counter()
        break
print("\n\n")
print("CLOSING CONNECTION")
CPU_Pct = str(round(float(os.popen('''grep 'cpu ' /proc/stat | awk '{usage=($2+$4)*100/($2+$4+$5)} END {print usage }' ''').readline()),2))
#print results
print("CPU Usage = " + CPU_Pct)
print("\n\n********************ELAPSED TIME: {} s".format(toc - tic))
print()
"""print(f"Max Frequency: {cpufreq.max:.2f}Mhz")
print(f"Min Frequency: {cpufreq.min:.2f}Mhz")
print(f"Current Frequency: {cpufreq.current:.2f}Mhz")"""
print()
print("CPU Usage Per Core:")
for i, percentage in enumerate(psutil.cpu_percent(percpu=True, interval=1)):
    print(f"Core {i}: {percentage}%")
krajnji_cpu = psutil.cpu_percent()
print(f"***********Total CPU Usage: {krajnji_cpu}%")

svmem = psutil.virtual_memory()
print(f"Total: {get_size(svmem.total)}")
print(f"Available: {get_size(svmem.available)}")
print(f"Used: {get_size(svmem.used)}")
print(f"***********Ram percentage: {svmem.percent}%")
print(krajnji_cpu, pocetni_cpu)
skok_cpu = krajnji_cpu - pocetni_cpu
skok_ram = svmem.percent - pocetni_ram

print(f"********************CPU skok: {skok_cpu}%")
print(f"********************RAM skok: {skok_ram}%")

print("="*20, "SWAP", "="*20)
# get the swap memory details (if exists)
swap = psutil.swap_memory()
"""print(f"Total: {get_size(swap.total)}")
print(f"Free: {get_size(swap.free)}")
print(f"Used: {get_size(swap.used)}")
print(f"Percentage: {swap.percent}%")"""

flag = 3
graph.BFS(graph.nodes[0])
#interate through the graph to close connections
print("Closing connections")
#graph.BFS(graph.nodes[0]) 
ServerSocket.close()
print("\n\nExit program\n")
sys.exit()
