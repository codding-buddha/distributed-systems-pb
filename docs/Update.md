## Update
Every update request/msg goes through different state before it is committed to stable store (boltdb in this scenario.). 
Note that server is free to reject a update request, so if client doesn't receive update reply in timely manner it can assume that request is lost or rejected.
When update request (req) arrives - 
1. Node checks if request is duplicate or is already in pending state, if it is, then it rejects the request, otherwise it moves the request to pending queue.
 Pending = Pending U {req}
2. Node then forwards this request to successor (depending on the node type), request is then moved to Sent state.
 Sent = Sent U {req} 
3. When tail processes this request, update request is executed and is committed to stable store. Then reply is sent to client, and ack msg is sent to predecessor.
4. When ack msg is received, then msg is committed to stable store and pending/sent states are updated, ack msg is then sent to predecessor till it reaches Head.
5. Pending/Sent data structure helps in recovery.

![SingleChain](./img/update-operation.gif)

|  Invariants |  |
:-------------------------:|:-------------------------:
| Pending state of  node's successor is a subset of that node's pending state.     | <img src="/docs/img/invariant1.png" alt="invariant1" width="250"/>|
| Entire Pending state of node is superset of its stable store|<img src="/docs/img/invariant2.png" alt="invariant2" height="250"/>|
| Stable store of a node's successor is superset of that node's stable store.|<img src="/docs/img/invariant3.png" alt="invariant3" width="250"/>|