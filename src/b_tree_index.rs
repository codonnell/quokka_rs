// What operations do we need to support?
// * get
// * get_key_value?
// * get_mut
// * iterator
// * append
// * range
// * range_mut?
// * remove
//
// To start, let's just use RwLock<BTreeMap>

use std::{collections::BTreeMap, sync::RwLock};

use crate::table::TupleSlot;

pub type BTreeIndex<K> = RwLock<BTreeMap<K, TupleSlot>>;
