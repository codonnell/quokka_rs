use anyhow::anyhow;
use roaring::RoaringBitmap;

const SLOTS_PER_BLOCK: usize = 1000;

type BlockIndex = usize;
type RowIndex = usize;

#[derive(Debug)]
pub struct TupleSlot {
    block_index: BlockIndex,
    row_index: RowIndex,
}

#[derive(Debug)]
pub struct Table {
    blocks: Vec<Block>,
}

impl Table {
    pub fn get_row(&self, tuple_slot: TupleSlot, column_ids: &[usize]) -> Option<ProjectedRow> {
        self.blocks
            .get(tuple_slot.block_index)?
            .row_at_index(tuple_slot.row_index, column_ids)
    }
}

#[derive(Debug)]
pub struct Block {
    num_slots: usize,
    num_records: usize,
    column_sizes: Vec<usize>,
    column_bytes: Vec<u8>,
    column_offsets: Vec<usize>,
    bitmaps: Vec<RoaringBitmap>,
    bitmap: RoaringBitmap,
}

impl Block {
    // For now, make all blocks have 1k slots
    pub fn new(column_sizes: Vec<usize>) -> Block {
        let mut bitmaps = Vec::with_capacity(column_sizes.len());
        for _ in column_sizes.iter() {
            bitmaps.push(RoaringBitmap::new());
        }
        let mut column_offsets = Vec::new();
        let mut column_offset = 0;
        let num_slots = SLOTS_PER_BLOCK;
        for size in column_sizes.iter() {
            column_offsets.push(column_offset);
            column_offset += num_slots * size;
        }
        Block {
            num_slots,
            num_records: 0,
            column_sizes,
            column_bytes: vec![0; column_offset],
            column_offsets,
            bitmaps,
            bitmap: RoaringBitmap::new(),
        }
    }

    pub fn insert(&mut self, row: &ProjectedRow) -> anyhow::Result<()> {
        if self.num_records == self.num_slots {
            return Err(anyhow!("cannot add a row to a full block"));
        }
        let record_index = self.num_records;
        let mut row_index = 0;
        for col_index in 0..self.column_sizes.len() {
            if row.column_ids[row_index] == col_index {
                match &row.column_values[row_index] {
                    Some(bytes) => {
                        let byte_index = self.column_offsets[col_index]
                            + record_index * self.column_sizes[col_index];
                        self.column_bytes[byte_index..(self.column_sizes[col_index] + byte_index)]
                            .copy_from_slice(&bytes[..self.column_sizes[col_index]]);
                        self.bitmaps[col_index].insert(record_index as u32);
                    }
                    None => {}
                }
                row_index += 1;
            }
        }
        self.num_records += 1;
        self.bitmap.insert(record_index as u32);
        Ok(())
    }

    pub fn update(&mut self, record_index: usize, row: &ProjectedRow) -> anyhow::Result<()> {
        if record_index >= self.num_records {
            return Err(anyhow!("cannot update a row that doesn't exist"));
        }
        for row_index in 0..row.column_ids.len() {
            let column_id = row.column_ids[row_index];
            match &row.column_values[row_index] {
                Some(bytes) => {
                    let byte_index = self.column_offsets[column_id]
                        + record_index * self.column_sizes[column_id];
                    self.column_bytes[byte_index..(self.column_sizes[column_id] + byte_index)]
                        .copy_from_slice(&bytes[..self.column_sizes[column_id]]);
                    self.bitmaps[column_id].insert(record_index as u32);
                }
                None => {
                    self.bitmaps[column_id].remove(record_index as u32);
                }
            }
        }
        Ok(())
    }

    pub fn delete(&mut self, record_index: usize) -> anyhow::Result<()> {
        if record_index >= self.num_records {
            return Err(anyhow!("cannot delete a row that doesn't exist"));
        }
        for bitmap in self.bitmaps.iter_mut() {
            bitmap.remove(record_index as u32);
        }
        for column_id in 0..self.column_sizes.len() {
            let size = self.column_sizes[column_id];
            let start_offset = self.column_offsets[column_id] + size * record_index;
            self.column_bytes[start_offset..start_offset + size].fill(0);
        }
        self.bitmap.remove(record_index as u32);
        // We do not decrement the number of records--that can be done during compaction
        Ok(())
    }

    pub fn row_at_index(&self, index: usize, column_ids: &[usize]) -> Option<ProjectedRow> {
        if index >= self.num_records {
            return None;
        }
        if !self.bitmap.contains(index as u32) {
            return None;
        }
        assert!(column_ids.is_sorted());
        let mut column_values = Vec::new();
        let mut has_value = false;
        for column_id in column_ids.iter() {
            if self.bitmaps[*column_id].contains(index as u32) {
                has_value = true;
                let size = self.column_sizes[*column_id];
                let start_offset = self.column_offsets[*column_id] + index * size;
                let value = self.column_bytes[start_offset..start_offset + size].to_vec();
                column_values.push(Some(value));
            } else {
                column_values.push(None);
            }
        }
        if has_value {
            Some(ProjectedRow {
                column_ids: column_ids.to_vec(),
                column_values,
            })
        } else {
            None
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ProjectedRow {
    column_ids: Vec<usize>,
    column_values: Vec<Option<Vec<u8>>>,
}

impl ProjectedRow {
    pub fn new(column_ids: Vec<usize>, column_values: Vec<Option<Vec<u8>>>) -> ProjectedRow {
        assert!(column_ids.is_sorted());
        ProjectedRow {
            column_ids,
            column_values,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::table::{Block, ProjectedRow};

    #[test]
    fn insert_and_get_projected_row() {
        let mut block = Block::new(vec![1, 2]);
        let row = ProjectedRow::new(vec![0, 1], vec![Some(vec![1]), Some(vec![1, 2])]);
        block.insert(&row).expect("block has space for a row");
        let out_row = block.row_at_index(0, &[0, 1]);
        assert_eq!(Some(row), out_row);
    }

    #[test]
    fn update_projected_row() {
        let mut block = Block::new(vec![1, 2]);
        let row = ProjectedRow::new(vec![0, 1], vec![Some(vec![1]), Some(vec![1, 2])]);
        block.insert(&row).expect("block has space for a row");
        let updated_row = ProjectedRow::new(vec![0, 1], vec![Some(vec![2]), Some(vec![3, 2])]);
        block
            .update(0, &updated_row)
            .expect("can find record to update");
        let out_row = block.row_at_index(0, &[0, 1]);
        assert_eq!(Some(updated_row), out_row);
    }

    #[test]
    fn delete_projected_row() {
        let mut block = Block::new(vec![1, 2]);
        let row = ProjectedRow::new(vec![0, 1], vec![Some(vec![1]), Some(vec![1, 2])]);
        block.insert(&row).expect("block has space for a row");
        block.delete(0).expect("can find a record to delete");
        let out_row = block.row_at_index(0, &[0, 1]);
        assert_eq!(None, out_row);
    }
}
