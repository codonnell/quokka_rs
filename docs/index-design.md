# Index Design

## Offset vs. Primary Key

Indexes can choose to store an offset to the memory location of the tuple in the table or the primary key of the tuple. This presents a design tradeoff. If we directly store an offset, we can locate the tuple without doing a primary key index lookup. However, if we change the tuple's offset, we need to update all indexes pointing to it. If an index stores the primary key, then we can transparently change the tuple's offset without updating any indexes.

If we use delta storage, then we should only need to move the tuple's offset when doing compaction. Since we expect changing the tuple's offset to be rare, we choose to store the tuple's table offset instead of primary key.

If the user updates an indexed attribute, we continue to store the old indexed key-value and add a new index entry for the new key-value. When the old version is garbage collected, then we also clean up any old index key-value entries.

If the user updates the primary key, then we do a delete and insert with the new primary key. This means the primary key index must support duplicates.

There is a good discussion of this in CMU's advanced database class [here](https://youtu.be/1Od_SuOQshM?si=V9OBArzq3EF9ogD3&t=4178).

## Planner

There are a couple of ways indexes could be integrated into the system for scanning tables efficiently. We'll leave off using them for joins for now.

### Hidden inside table provider

We could look for an appropriate index in our custom table provider and use one if available. This has the disadvantage of not being visible to the physical plan optimizer, but requires less work to implement.

### New index scan / index join execution plan

We could add an index scan execution plan. The physical plan optimizer can then take it into account.

# Custom B Tree Index

## Problems with std::collections::BTreeMap

* Doesn't support range scans (iterator starting at a certain key)
  * This is actually possible using the `btree_cursors` experimental API (see the [github issue](https://github.com/rust-lang/rust/issues/107540))
* Doesn't support concurrent access from multiple threads
* Doesn't support duplicate keys
  * From [Graefe's B-Tree Survey](https://w6113.github.io/files/papers/btreesurvey-graefe.pdf) page 224, can add a unique identifier to key when non-unique, like a ctid.
  * If sort order uses the column value first, can use an iterator to find the right value.

## Custom B Tree Design

* Sibling pointers between leaf nodes to enable fast iteration
* Thread safe using optimistic latch crabbing with something like `parking_lot::RwLock`.
* Supports duplicate keys? Or maybe we use the extra identifier instead.

We need to be careful about how we store our B tree in memory. It is better to store values in nodes rather than pointers, as it allows for fewer cache misses in the CPU. This implies that we should allocate a fixed length array per tree node. However, we do not know the size of any given index at compile time--it is dynamically determined at runtime based on the type of the column. Arrow solves a similar problem by internally allocating a byte butter and interpreting its data dynamically using reflection. I think it should be possible to do something similar and store nodes as a `Vec`.

It also begs the question of what we should do for variable length columns like strings. The most efficient option is probably to do the "German style layout" described in section 3.1 of [this paper](https://db.in.tum.de/~freitag/papers/p29-neumann-cidr20.pdf).
