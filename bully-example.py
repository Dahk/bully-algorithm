"""
Students: Pol Roca Llaberia - pol.roca@estudiants.urv.cat
          Marc Bachs Ciprés - marc.bachs@estudiants.urv.cat
Subject: Sistemes Distribuits (Distributed Systems)
"""

import pywren_ibm_cloud as pywren
import json, os, pika, yaml, random, sys, time

# implementation of 'the bully' algorithm with a state machine, switch case-like logic
# the highest id process available gets chosen, and others get notified
def chooseLeader (ownId, idList, first_to_attempt, channel):
    if first_to_attempt:
        # fire the first message to start the election process
        channel.basic_publish(exchange='', routing_key=f'worker-queue-{ownId}', body='Election:-1')
    else:
        time.sleep(0.3)  # wait to receive, so as not to spam at first

    state = 0   # initial state
    election_timeout = 0
    qstr = 'worker-queue-{}'
    
    # state transitions - when message X is received, change to state Y
    def s_zero (msg):
        nonlocal state
        if msg[:8] == 'Victory:':
            state = 1
        elif msg[:9] == 'Election:':
            state = 2
            # vv Disclaimer: not the casual functionality vv
            if not first_to_attempt and (ownId > max(idList)):
                state = 0   # pretend we're dead, don't answer
            else:
                state = 2
            # ^^ Disclaimer: not the casual functionality ^^
    def s_one (msg):
        return
    def s_two (msg):
        return
    def s_three (msg):
        nonlocal state
        if msg[:8] == 'Victory:':
            state = 1
    def s_four (msg):
        return
    def s_five (msg):
        nonlocal state, election_timeout
        if msg == 'Answer':
            state = 3
        elif msg[:8] == 'Victory:':
            state = 1
        else:
            # begin to count the timeout for a response
            election_timeout = time.time() if not election_timeout else election_timeout
            # promote ourselves to leader after 3s with no response
            state = 4 if (time.time() - election_timeout) > 3 else 5

    # do-on-entry functions, to a state
    def f_zero (msg):
        return -1
    def f_one (msg):
        return int(msg[8:])
    def f_two (msg):
        nonlocal state
        answer_id = int(msg[9:])
        if answer_id != -1:   # -1 = initial firing message
            channel.basic_publish(exchange='', routing_key=qstr.format(answer_id), body='Answer')

        higherIds = [i for i in idList if i > ownId]
        if higherIds:   # if we're not the highest id
            for i in higherIds:
                channel.basic_publish(exchange='', routing_key=qstr.format(i), body=f'Election:{ownId}')
            state = 5
        else:   # we're the highest id
            state = 4
        return -1
    def f_three (msg):
        if msg[:9] == 'Election:':
            channel.basic_publish(exchange='', routing_key=qstr.format(msg[9:]), body='Answer')
        return -1
    def f_four (msg):
        vmsg= f'Victory:{ownId}'
        for i in idList:
            channel.basic_publish(exchange='', routing_key=qstr.format(i), body=vmsg)
        return ownId
    def f_five (msg):            
        if msg[:9] == 'Election:':
            channel.basic_publish(exchange='', routing_key=qstr.format(msg[9:]), body='Answer')
        return -1

    # create the 'state machine'-like lists (python switch case pattern)
    stateMachine = [s_zero, s_one, s_two, s_three, s_four, s_five]
    onEntryFunc = [f_zero, f_one, f_two, f_three, f_four, f_five]

    ownQueue = qstr.format(ownId)
    chosen = -1
    while chosen < 0:
        msg = channel.basic_get(queue=ownQueue, no_ack=True)[2] # body of the message
        if msg: # if the queue was not empty
            msg = msg.decode('utf-8')
        else:
            msg = 'None'
        stateMachine[state](msg)    # do transition
        chosen = onEntryFunc[state](msg)    # execute state entry function
        if msg == 'None':
            time.sleep(1)  # so as not to spam pull requests
    return chosen

# callback for reading and generating messages
class ReaderCallback:
    def __init__(self, idList):
        self.result = []
        self.idList = idList
        self.counter = 1
    def setCounter(self, c):
        self.counter = c
    def __call__(self, ch, method, properties, body):
        msg = body.decode('utf-8')

        if msg == 'roll':
            r = random.randint(0, 1000)
            self.result.append(r)
            for i in self.idList:
                ch.basic_publish(exchange='', routing_key=f'worker-queue-{i}', body=str(r))
        else:
            try:
                msg = int(msg)
                self.result.append(msg)
            except: # junk message, ignore it
                self.counter + 1

        self.counter = self.counter - 1
        if not self.counter:
            ch.stop_consuming()

# something to do synchronously, in this case write a list of random numbers altogether
def sync_work(ownId, idList, leader, ch):
    reader = ReaderCallback(idList.copy())
    idList.append(ownId)
    ownQueue = f'worker-queue-{ownId}'
    if leader == ownId:
        # randomly choose workers to generate random numbers
        for i in random.sample(idList, k=len(idList)):
            queueId = f'worker-queue-{i}'
            ch.basic_publish(exchange='', routing_key=queueId, body='roll')

            reader.setCounter(1)
            ch.basic_consume(reader, queue=ownQueue, no_ack=True)
            ch.start_consuming()
        reader.result.append(f'Bully id: {leader}')
    else:
        # wait for other's random messages or for the leader to choose us
        reader.setCounter(len(idList))
        ch.basic_consume(reader, queue=ownQueue, no_ack=True)
        ch.start_consuming()
    return reader.result

# worker function, mapper
def worker(args):
    pw_config = json.loads(os.environ.get('PYWREN_CONFIG', ''))
    pika_params = pika.URLParameters(pw_config['rabbitmq']['amqp_url'])
    connection = pika.BlockingConnection(pika_params)
    channel = connection.channel()
    
    first_to_attempt = args['worker_id'] < 0
    ownId = abs(args['worker_id'])
    idList = args['id_list']
    idList.remove(ownId)    # most of the time we don't want to count ourselves
    idList.reverse()    # assuming it was sorted; the election process gets faster if we send to the higher ids first

    # choose leader in a distributed way, using the bully algorithm with a state machine implementation
    leader = chooseLeader(ownId, idList, first_to_attempt, channel)
    # do the synchronous work
    result = sync_work(ownId, idList, leader, channel)

    channel.close()
    return result

if __name__ == '__main__':
    with open('~/.pywren_config', 'r') as f:    # check this path (esp. on Windows)
        secret = yaml.safe_load(f)
    pika_params = pika.URLParameters(secret['rabbitmq']['amqp_url'])
    connection = pika.BlockingConnection(pika_params)
    channel = connection.channel()

    # prepare arguments (id list)
    n = int(sys.argv[1]) if len(sys.argv) == 2 else 10
    id_list = [i for i in range(1, n+1)]
    iterdata = []
    ql = []
    for i in id_list:
        queueId = f'worker-queue-{i}'
        channel.queue_declare(queue=queueId)
        iterdata.append([{
            'worker_id': i,
            'id_list': id_list
        }])
        ql.append(queueId)

    # pick up a random id which will start the election process
    choice = random.choice(iterdata)
    choice[0]['worker_id'] *= -1

    # execute workers
    pw = pywren.ibm_cf_executor(rabbitmq_monitor=True)
    pw.map(worker, iterdata)
    result = pw.get_result()

    # print result and delete all queues
    print('\n== Arbitrary id:', -choice[0]['worker_id'], '==')
    [print(r) for r in result]
    print('=====================')
    print('\nDeleting queues... ')
    for i in ql:
        channel.queue_delete(i)
    print('Done.')

    channel.close()