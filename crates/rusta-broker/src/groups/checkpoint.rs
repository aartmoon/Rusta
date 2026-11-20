use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, ErrorKind, Read, Write};
use std::path::Path;

use crc32c::crc32c;
use serde::{Deserialize, Serialize};

use super::store::{Key, Offset, OffsetBudget};
use super::{Group, MAX_GROUPS, valid_id};

const FILE: &str = "state.checkpoint";
const MAGIC: &[u8; 8] = b"RUSTAG01";
const HEADER: usize = 24;
const MAX_RECORD_BYTES: usize = 1024 * 1024;

pub fn load(path: &Path, first: u64, end: u64) -> io::Result<(u64, HashMap<String, Group>)> {
    let file = match File::open(path.join(FILE)) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound && first == 1 => {
            return Ok((first, HashMap::new()));
        }
        Err(error) => return Err(error),
    };
    let mut reader = BufReader::new(file);
    let mut header = [0; HEADER];
    reader.read_exact(&mut header)?;
    let next = u64::from_be_bytes(header[8..16].try_into().unwrap());
    let count = u32::from_be_bytes(header[16..20].try_into().unwrap()) as usize;
    let checksum = u32::from_be_bytes(header[20..24].try_into().unwrap());
    if &header[..8] != MAGIC
        || crc32c(&header[..20]) != checksum
        || !(first..=end).contains(&next)
        || count > MAX_GROUPS
    {
        return Err(invalid());
    }
    let mut groups = HashMap::new();
    let mut payload = Vec::new();
    for _ in 0..count {
        let mut prefix = [0; 8];
        reader.read_exact(&mut prefix)?;
        let size = u32::from_be_bytes(prefix[..4].try_into().unwrap()) as usize;
        let checksum = u32::from_be_bytes(prefix[4..].try_into().unwrap());
        if size > MAX_RECORD_BYTES {
            return Err(invalid());
        }
        payload.resize(size, 0);
        reader.read_exact(&mut payload)?;
        if crc32c(&payload) != checksum {
            return Err(invalid());
        }

        let entry: StoredGroup = serde_json::from_slice(&payload)?;
        if !valid_id(&entry.id) || entry.generation < 0 || groups.contains_key(&entry.id) {
            return Err(invalid());
        }
        let mut group = Group {
            generation: entry.generation,
            empty_since: entry.empty_since,
            ..Group::default()
        };
        let mut budget = OffsetBudget::new(&group.offsets);
        for (key, value) in &entry.offsets {
            budget.admit(*key, value).map_err(|_| invalid())?;
        }
        group.offsets.extend(entry.offsets);
        groups.insert(entry.id, group);
    }
    if reader.read(&mut [0])? != 0 {
        return Err(invalid());
    }
    Ok((next, groups))
}

#[derive(Deserialize)]
struct StoredGroup {
    id: String,
    generation: i32,
    empty_since: Option<u64>,
    offsets: Vec<(Key, Offset)>,
}

#[derive(Serialize)]
struct GroupRef<'a> {
    id: &'a str,
    generation: i32,
    empty_since: Option<u64>,
    offsets: Vec<(&'a Key, &'a Offset)>,
}

pub fn save(path: &Path, next: u64, groups: &HashMap<String, Group>) -> io::Result<()> {
    let temporary = path.join("state.checkpoint.tmp");
    let mut writer = BufWriter::new(File::create(&temporary)?);
    let groups = groups
        .iter()
        .filter(|(_, group)| group.generation != 0 || !group.offsets.is_empty());
    let count = groups.clone().count() as u32;
    let mut header = [0; HEADER];
    header[..8].copy_from_slice(MAGIC);
    header[8..16].copy_from_slice(&next.to_be_bytes());
    header[16..20].copy_from_slice(&count.to_be_bytes());
    let checksum = crc32c(&header[..20]);
    header[20..24].copy_from_slice(&checksum.to_be_bytes());
    writer.write_all(&header)?;

    let mut payload = Vec::new();
    for (id, group) in groups {
        payload.clear();
        serde_json::to_writer(
            &mut payload,
            &GroupRef {
                id,
                generation: group.generation,
                empty_since: group.empty_since,
                offsets: group.offsets.iter().collect(),
            },
        )?;
        if payload.len() > MAX_RECORD_BYTES {
            return Err(invalid());
        }
        writer.write_all(&(payload.len() as u32).to_be_bytes())?;
        writer.write_all(&crc32c(&payload).to_be_bytes())?;
        writer.write_all(&payload)?;
    }

    writer.flush()?;
    writer.get_ref().sync_all()?;
    fs::rename(temporary, path.join(FILE))?;
    File::open(path)?.sync_all()
}

fn invalid() -> io::Error {
    io::Error::new(ErrorKind::InvalidData, "invalid group checkpoint")
}
