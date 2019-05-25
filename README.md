# The bully algorithm on a distributed synchronous system

## How to run:
The execution of this program is pretty straight forward:

```
python3 bully-example.py <num_workers>
```
```
python3 bully-example.py 5
python3 bully-example.py 18
```
By the way, <num_workers> is optional and will be set to 10 if it is not specified.
You may also check the path to the *.pywren_config* file (line 180).
## Dependencies:
In order to run this program you must have installed the following dependencies:

- [pywren_ibm_cloud](https://github.com/pywren/pywren-ibm-cloud) (and the file .pywren_config must be set up)
- [pika](https://github.com/pika/pika)
- [pyyaml](https://pyyaml.org/wiki/PyYAMLDocumentation)

And of course an IBM Cloud account with access to IBM Cloud Functions and RabbitMQ services.

*Note: pywren_ibm_cloud doesn't use the latest pika version.*

---
## Explanation
This program is based on the bully algorithm implementation explained at the wikipedia page of the method 
([Bully algorithm](https://en.wikipedia.org/wiki/Bully_algorithm/)).

This method of selecting a leader would be executed when a new leader needs to be chosen because
the last one disconnected, for example. In our case, in order to start this process we decided to 
randomly pick a worker before dispatching them, and that will be the one to fire the first election
message to trigger the process.

One important note to take is that the program on the github repository has a small modification on the code.
The casual functionality of the program is to dispatch n workers to the cloud (ibm cloud, using pywren) and those
will pick the highest id worker as a leader, which will be the one to tell who has to generate the random number
for the list. However, we have modified this default logic so that the real case where this method shines can be
seen when we run it. 

The modification we made is on the state machine (function chooseLeader) and what it basically does it prevents 
the worker with the highest id from answering the election messages. This way, we can simulate a scenario where 
one node has crashed or is not attending requests. Therefore, in the output of every execution we should notice
that the correct result, the selected leader, is the n-1 worker.
This small modification can be easily seen in the code as it is marked with comments. 

*Also, if you wish to see the regular execution of the algorithm, you just have to delete 
the section marked by the disclaimer comments (ctrl-f ‘disclaimer’).*


## Authors
Pol Roca Llaberia (<pol.roca@estudiants.urv.cat>)

Marc Bachs Ciprés (<marc.bachs@estudiants.urv.cat>)

## License
This project is licensed under the MIT License - see the [LICENSE](/LICENSE) file for details.
