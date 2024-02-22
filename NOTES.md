# Quokka Search

## Goals

Quokka Search (QS) aims to efficiently support facet and full text search across multiple tables. We hope to accomplish this by keeping data to a size where it can fit in memory on a single machine, maybe even where all of the data can fit in memory.

* Support faceted search
* Support full text search
* Support efficient joins

## Non-Goals

* Support massive scale (maybe even recommend all data fits in memory)
* Support transactions

## Ideas

* Should we ask for a partition key, targeting multi-tenant implementations?
* Should we use io_uring for fast I/O? The only I/O will be writing WAL/checkpoints and reading WAL/checkpoint on startup.
* We could set up a postgres logical replication subscriber functionality to make it extremely easy to ingest data from postgres
* Should we implement [Column Sketches](https://15721.courses.cs.cmu.edu/spring2023/papers/04-olapindexes/hentschel-sigmod18.pdf)?
* [Hekaton](https://www.microsoft.com/en-us/research/wp-content/uploads/2013/06/Hekaton-Sigmod2013-final.pdf) or [this paper](https://www.vldb.org/pvldb/vol10/p781-Wu.pdf) might be interesting inspiration for MVCC
* We should use [Apache Arrow DataFusion](https://arrow.apache.org/datafusion/) so we don't need to reimplement a query engine from scratch.

## Questions

* Should we start with a prebuilt disk/memory format like Apache Arrow/Feather/Parquet/ORC?
  * Planning to use the storage layout described in [this paper](https://db.cs.cmu.edu/papers/2020/p534-li.pdf)
* Should Quokka Search be an in-memory system?
  * Yes
* How much throughput can a single writer thread achieve on modern hardware?
  * Redis has a single writer thread and can handle high write load. The bottleneck for writes will likely still be I/O.
* How do we add/remove columns?
  * Probably need a table lock for this. When adding a column, will need to shuffle tuples between blocks because tuples will be bigger.

## Why Quokka Search over Postgres

* Asynchronous indexing
  * Can prioritize read performance over write performance when under load
  * Batching writes may improve throughput
* Single writer thread?
  * Further reduces concurrency overhead
  * Sufficient for most use cases
* No constraint support
  * Hopefully writes can be faster if we don't need to check constraints (other than primary key)
* No support for user-facing transactions
  * Only need internal transaction support for atomic writes
  * Don't need to worry about a transaction interleaving reads and writes
  * Should simplify concurrency concerns, increasing speed
* Uses modern APIs (like io_uring), improving speed
* In memory database removes a lot of overhead for swapping between memory and disk 
  * [This paper](http://nms.csail.mit.edu/~stavros/pubs/OLTP_sigmod08.pdf) counted 7% of instructions went to "useful work"

## Decisions
* MVCC?
  * Optimistic Concurrency Control vs. Timestamp Ordering vs. Two Phase Locking
  * Storage
    * Append only (newest or oldest first?)
    * Time travel storage 
    * Delta storage

* Let's do delta storage--it jives with the lightweight profile we're going for, and based on [this paper](https://15721.courses.cs.cmu.edu/spring2020/papers/03-mvcc1/wu-vldb2017.pdf) it performs very well even though it might be expected to have slower reads.

## Concurrency (assuming single thread writer)
* Writer blindly attempts to run its insert/update/delete, which may fail
  * No need to worry about locking because there can be no write conflicts
* Readers
  * When started, identify txids below them (that haven't been GCed) that are in progress or aborted
  * Only read tuples with versions lower than their txid and not in the above list

The tricky part of this is how to manage in-progress and aborted write transactions. If read transactions only had to iterate through committed version tuples, it would be straightforward to achieve serializable operation. Perhaps we could take the Hekaton approach of using a bit in the transaction ids to indicate whether they have been committed? This is not enough--it would be possible to read data from before the write transaction committed and after it committed from the same read transaction.

### Proposed Solution

* Keep central data structure with aborted and in-progress transactions that have not been GCed. Copy this when a read transaction starts.
  * Can we get away with just the in-progress write transaction?
* Use a bit per tuple to track whether the version has been committed.

Each read transaction reads the committed tuple with the largest transaction id below its own that is not the in-progress transaction.

### Alternative Solution 

When a read transaction starts, copy the current write transaction id. Only read committed tuples with ids below the copied write transaction id. This is a variation of timestamp ordering.

### Serializable Check 

Read transactions will always be ordered between the write transaction that committed previous to when the read transaction started and the subsequent write transaction. Write transactions are serially ordered by virtue of being executed on a single thread. As long as these two invariants hold, the system will run at a serializable isolation level.

## Serialization/Deserialization of Data Files

[This](https://nathancraddock.com/blog/deserialization-with-zig-metaprogramming/) is a very useful blog post on the topic. We should be able to convert structs between bytes and structs, though we may need to use packed structs and be careful about alignment to avoid unaligned loads. (CPUs deal better with values aligned 1/2/4/8 byte boundaries.) Need to look more into alignment.

## Useful Resources

* [Rustonomicon Atomics](https://doc.rust-lang.org/nomicon/atomics.html)

## papers

* [TigerBeetle paper list](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/DESIGN.md)

## Table Layout

Borrowing from the layout of [noisepage](https://github.com/cmu-db/noisepage/blob/master/docs/design_storage.md), which is based off [this paper](https://15721.courses.cs.cmu.edu/spring2018/papers/06-mvcc2/p677-neumann.pdf). Its relationship with Apache Arrow is documented in [this paper](https://db.cs.cmu.edu/papers/2020/p534-li.pdf).

Do we need to store the number of attributes and their sizes in a block? For the database, I don't think so. Is there a problem with getting that information from the catalog? Maybe MVCC while the catalog is changing? Maybe it's in the layout to match Apache Arrow?

## Catalog

The catalog will start with just three tables:
* qs_tables
* qs_columns
* qs_types

# DataFusion

How can we layer transactions and indexes on top of the base query engine?

We can add a transaction id into `SessionConfig` using a custom extension (added via `SessionConfig::with_extension`).

We can write a custom table provider similar to `MemTable` that returns the right record batches given the transaction id in session config. It also updates the MVCC bits when inserting new rows.

The `DefaultPhysicalPlanner` doesn't currently support creating a plan with a delete. We'll need to figure out how to add that eventually.

A logical join is converted to a nested loop, hash, or sort merge join in `physical_planner.rs` `DefaultPhysicalPlanner.create_initial_plan`. Not sure how to adjust that to use index joins.

We'll need to add new logical plan nodes for various index lookups. Will probably want to add a primary key index to tables, as well, at the very least to allow upserts to update data.

## Create Table

The logical plan generated by datafusion for create table statements doesn't support primary keys on tables. So our function that takes a statement and turns it into a logical plan can look for a create table statement and handle it specially, then pass everything else to the datafusion logical plan creator. The `sqlparser` library backing most of datafusion's parser _does_ support adding a primary key to a table.
