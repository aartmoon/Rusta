use std::fs::{self, File};
use std::io::{self, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::format::{Entry, HEADER_LEN, Position, corrupt, read_entry};

const INDEX_INTERVAL: u64 = 16 * 1024;
const MAX_INDEX_POINTS: usize = 128;
pub(crate) const MAX_SEGMENTS: usize = 16_384;

#[derive(Clone, Copy)]
pub(crate) struct Cursor {
    pub position: Position,
    pub byte: u64,
}

pub(crate) struct Segment {
    pub start: Position,
    pub end: Position,
    pub bytes: u64,
    pub last_term: u64,
    pub max_append_timestamp: i64,
    pub max_record_timestamp: i64,
    index: Vec<Cursor>,
    stride: u64,
}

impl Segment {
    pub fn new(start: Position) -> Self {
        Self {
            start,
            end: start,
            bytes: 0,
            last_term: 0,
            max_append_timestamp: i64::MIN,
            max_record_timestamp: i64::MIN,
            index: Vec::new(),
            stride: INDEX_INTERVAL,
        }
    }

    pub fn push(&mut self, entry: Entry, next: Position, max_record_timestamp: i64) {
        if self
            .index
            .last()
            .is_none_or(|last| self.bytes - last.byte >= self.stride)
        {
            if self.index.len() == MAX_INDEX_POINTS {
                let mut index = 0;
                self.index.retain(|_| {
                    index += 1;
                    index % 2 == 1
                });
                self.stride = self.stride.saturating_mul(2);
            }
            self.index.push(Cursor {
                position: entry.position,
                byte: self.bytes,
            });
        }
        self.end = next;
        self.bytes += HEADER_LEN + u64::from(entry.payload_len);
        self.last_term = entry.term;
        self.max_append_timestamp = self.max_append_timestamp.max(entry.append_timestamp_ms);
        self.max_record_timestamp = self.max_record_timestamp.max(max_record_timestamp);
    }

    pub fn scan(&self, root: &Path, index: u64, offset: i64) -> io::Result<Scan> {
        let i = self.index.partition_point(|point| {
            point.position.next_index <= index && point.position.next_offset <= offset
        });
        let cursor = self.index[i.saturating_sub(1)];
        Scan::new(&path(root, self.start.next_index), cursor, self.bytes)
    }
}

pub(crate) struct Scan {
    reader: BufReader<File>,
    cursor: Cursor,
    file_len: u64,
}

impl Scan {
    pub fn new(path: &Path, cursor: Cursor, file_len: u64) -> io::Result<Self> {
        let mut reader = BufReader::new(File::open(path)?);
        reader.seek(SeekFrom::Start(cursor.byte))?;
        Ok(Self {
            reader,
            cursor,
            file_len,
        })
    }

    pub fn byte_position(&self) -> u64 {
        self.cursor.byte
    }

    pub fn next(&mut self, payload: &mut Vec<u8>) -> io::Result<Option<Entry>> {
        if self.cursor.byte == self.file_len {
            return Ok(None);
        }
        let entry = read_entry(
            &mut self.reader,
            self.file_len - self.cursor.byte,
            self.cursor.position,
            payload,
        )?;
        self.cursor.byte += HEADER_LEN + u64::from(entry.payload_len);
        self.cursor.position = self
            .cursor
            .position
            .after(entry.record_count)
            .map_err(|_| corrupt("entry position overflow"))?;
        Ok(Some(entry))
    }
}

pub(crate) fn path(root: &Path, index: u64) -> PathBuf {
    root.join(format!("{index:020}.log"))
}

pub(crate) fn list(root: &Path) -> io::Result<Vec<u64>> {
    let mut indexes = Vec::new();
    for item in fs::read_dir(root)? {
        let item = item?;
        let name = item.file_name();
        let Some(index) = name.to_str().and_then(|name| name.strip_suffix(".log")) else {
            continue;
        };
        if index.len() != 20
            || !index.bytes().all(|b| b.is_ascii_digit())
            || !item.file_type()?.is_file()
        {
            return Err(corrupt("invalid segment filename or file type"));
        }
        indexes.push(
            index
                .parse::<u64>()
                .map_err(|_| corrupt("invalid segment index"))?,
        );
        if indexes.len() > MAX_SEGMENTS {
            return Err(corrupt("segment count exceeds storage limit"));
        }
    }
    indexes.sort_unstable();
    Ok(indexes)
}
