# MVCC Design

## Assumptions

* We have a single thread doing writes serially
* This is an in-memory database

## Timestamp Ordering

We use a variant of timestamp ordering (as opposed to optimistic concurrency control or two phase locking) optimized for our in-memory, single writer thread setup.

We maintain an atomic _writer transaction id_ integer. When a write transaction commits, the single writer thread increments the writer transaction id. It tags all of its updates in tables with its transaction id--this is the timestamp from timestamp ordering.

When a read query starts, we copy the current write transaction id. The read query only reads tuples committed with ids below its copied write transaction id.

We maintain one bit in the tuple's timestamp id to track whether the update was committed or not. We assume each transaction will commit, keep track of the tuples and indexes we've updated, and change the bit and adjust the indexes to roll back if necessary. We don't support constraints other than a primary key constraint, so we expect transactions to roll back rarely.

## Delta Storage

We use delta storage (as opposed to time travel or append only) with newest first. This means that the tuple in the table has the latest version, and the delta storage has a sequence of diffs to older versions. We may be able to store these delta records directly to the WAL for durability. We could potentially save some memory by allocating the delta storage per transaction in a block, so we can serialize that block directly to disk to commit the transaction.

## Garbage Collection

We will employ an epoch-based garbage collection scheme. This requires tracking active read transaction ids. An initial tracing mechanism might keep a minimum transaction id and a ring buffer, where each item tracks the number of active read queries with that transaction id. When we're ready to garbage collect, we walk the ring buffer until we reach a non-zero entry. Each element we walked past is an invisible transaction, and we garbage collect its data.

If we allocate delta storage in a single block per transaction, we can quickly deallocate the delta storage for any transactions that are no longer visible, though we will also need to clean up any old index entries.
