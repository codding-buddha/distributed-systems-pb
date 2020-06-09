# Fault Tolerant Key Value Store
Implementation of fault tolerant  KeyValue service using chain-replication. 
This is project is more focused on chain-replication technique, it uses [boltdb](https://github.com/etcd-io/bbolt) for the underlying KeyValue storage.


| Replication Technique              | Failure Model | Consistency Model                          |
|------------------------------------|---------------|--------------------------------------------|
| Chain Replication                  | Fail Stop     | Strong                                     |


## Chain Replication
The chain replication algorithm is a variation of master/slave (primary/backup)
replication where all servers that are responsible for storing a
replica of an object that are arranged in a strictly-ordered chain. The
head of the chain makes all decisions about updates to an object.
The head’s decision is propagated down the chain in strict order.
The number of replicas for an object is determined by the length
of the replica chain that is responsible for that object. To tolerate f
replica server failures, a chain must be at least f + 1 servers long.
Operations on objects within the chain are linearizable when all updates are processed by the head of the
chain and all read-only queries are processed by the tail of the
chain. Strong consistency is maintained because read requests are
processed only by the tail of the chain.



|![Chain](./docs/img/chain-replication.png#thumbnail)  |  ![Primary_Backup](./docs/img/primary-backup.png#thumbnail)|
|:---:|:---:|
| Chain Replication| Primary Backup |

## Implementation

### Single Chain

* Reply - The reply of every request is sent by tail.
* [Query](./docs/Query.md) - Every query request is directed to the tail of the chain and processed there atomically.
* [Update](./docs/Update.md) - Each update request is directed to head of the chain.The request is processed atomically and then forwarded along a **reliable FIFO link** to the next element of the chain (where it is handled and forwarded), and so on until the request is handled by the tail. 

#### [Coping With Server Failures](./docs/Failure.md)
In order to detect failures of server that is part of the chain (fail-stop assumption, so all failures will be detected), and reconfigure the chain by eliminating failed the server, A special service is employed, called **master** that
* detects failure of server.
* reconfigures the chain by informing each server in the chain of its new predecessor or new successor in the new chain obtained  by deleting the failed server.
* informs clients about (or book keeping of) current head & tail.

![SingleChain](./docs/img/single-chain.png)

### Multi Chain

For large scale store we would like to partition key sets among multiple chains. To manage partitions and map a given key to particular chain, Dispatcher service is used.
Client sends all its request to Dispatcher, however update replies are sent by tail of the given chain directly.

<img src="/codding-buddha/distributed-systems-pb/raw/master/docs/img/single-chain.png" width="600px"/>

### Comparison, Limitation & Misc
Obvious short-coming of chain replication is latency when compared to typical primary-backup approach as request needs to go through entire chain, however unlike primary-backup tail can respond to query resulting in better throughput. Also, for strong consistency not all servers are utilized for query requests.
Comparison of weak chain variant with typical chain-replication can be found below. Total 25 clients with around 500 mixed query/update requests in parallel.

|  T = 2             |  T = 5 |  T = 10 |
:-------------------------:|:-------------------------:|:-------------------------:
|![t2](./docs/img/t2.png)| ![t5](./docs/img/t5.png)| ![t10](./docs/img/t10.png) |

It is evident from the experiments that for workloads with almost equal mix of update and query request, strongly consistent chain-replication performs identical or even better compared to weak variant.
Still client workloads with extremely large read/write ratios can potentially imbalance individual server workloads: 100% of read operations are sent to the same server, the tail of the chain.
* Tail is bottleneck - Possible optimization, allow read from  clean nodes (CRAQ).
* Supports only strong consistency - Eventual consistency is option with changes in chain configuration (CRAQ). Casual consistency is also possibility with smart clients (Chain Reaction)
* Requires Master 

### Build, Running Tests & Code
```
go build .
go test github.com/codding-buddha/ds-pb/tests/service
```
```
docker-compose up // run chain with 3 nodes with master running at localhost:9000
```
```
// run master
~ go run main.go -mode=master -maddr=localhost:9000

// add node to chain, master running at localhost:9000
~ go run main.go -mode=chain -addr=localhost:9001 -maddr=localhost:9000

// run client
~go run main.go -mode=client -addr=localhost:9004 -maddr=localhost:9000
```