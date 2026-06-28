use crate::formats::{
    Hash32,
    git::{GitHashAlgorithm, GitSourceRecord},
    hash32,
};
use anyhow::{Result, anyhow, bail};
use flate2::read::ZlibDecoder;
use sha1::{Digest, Sha1};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedGitPackIndex {
    pub hash_algorithm: GitHashAlgorithm,
    pub pack_hash: Hash32,
    pub records: Vec<GitSourceRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GitObjectKind {
    Commit,
    Tree,
    Blob,
    Tag,
}

impl GitObjectKind {
    fn name(self) -> &'static str {
        match self {
            Self::Commit => "commit",
            Self::Tree => "tree",
            Self::Blob => "blob",
            Self::Tag => "tag",
        }
    }
}

#[derive(Debug, Clone)]
struct GitPackObject {
    kind: GitObjectKind,
    object_id: Vec<u8>,
    entry_offset: u64,
    entry_len: u64,
    data: Vec<u8>,
}

#[derive(Debug, Clone)]
struct RawGitPackEntry {
    entry_offset: u64,
    entry_len: u64,
    object: RawGitObject,
}

#[derive(Debug, Clone)]
enum RawGitObject {
    Base { kind: GitObjectKind, data: Vec<u8> },
    OffsetDelta { base_offset: u64, delta: Vec<u8> },
    RefDelta { base_id: Vec<u8>, delta: Vec<u8> },
}

pub fn build_git_source_index_from_pack(
    repository_id: &str,
    pack_bytes: &[u8],
    pack_object_version_id: [u8; 16],
) -> Result<ParsedGitPackIndex> {
    let objects = parse_sha1_pack(pack_bytes)?;
    let by_id = objects
        .iter()
        .map(|object| (object.object_id.clone(), object))
        .collect::<BTreeMap<_, _>>();
    let commits = objects
        .iter()
        .filter(|object| object.kind == GitObjectKind::Commit)
        .collect::<Vec<_>>();
    let mut records = Vec::new();
    for commit in commits {
        let tree_id = commit_root_tree_id(&commit.data)?;
        let mut visited_trees = BTreeSet::new();
        collect_tree_records(
            repository_id,
            &by_id,
            &mut visited_trees,
            &commit.object_id,
            &tree_id,
            "",
            pack_object_version_id,
            &mut records,
        )?;
    }
    Ok(ParsedGitPackIndex {
        hash_algorithm: GitHashAlgorithm::Sha1,
        pack_hash: hash32(pack_bytes),
        records,
    })
}

fn parse_sha1_pack(pack_bytes: &[u8]) -> Result<Vec<GitPackObject>> {
    if pack_bytes.len() < 12 + 20 {
        bail!("git pack is too short");
    }
    if &pack_bytes[..4] != b"PACK" {
        bail!("git pack magic mismatch");
    }
    let version = u32::from_be_bytes(pack_bytes[4..8].try_into().unwrap());
    if version != 2 && version != 3 {
        bail!("unsupported git pack version {version}");
    }
    let expected_count = u32::from_be_bytes(pack_bytes[8..12].try_into().unwrap()) as usize;
    let data_end = pack_bytes.len() - 20;
    let mut hasher = Sha1::new();
    hasher.update(&pack_bytes[..data_end]);
    let trailer = hasher.finalize();
    if &trailer[..] != &pack_bytes[data_end..] {
        bail!("git pack checksum mismatch");
    }

    let mut offset = 12;
    let mut raw_entries = Vec::with_capacity(expected_count);
    for _ in 0..expected_count {
        let entry_offset = offset;
        let (kind, size, header_len) = read_object_header(pack_bytes, offset, data_end)?;
        offset = offset
            .checked_add(header_len)
            .ok_or_else(|| anyhow!("git pack offset overflow"))?;
        let object = match kind {
            RawGitObjectKind::Base(kind) => {
                let (data, compressed_len) = inflate_object(&pack_bytes[offset..data_end])?;
                if data.len() as u64 != size {
                    bail!("git pack object inflated size mismatch");
                }
                offset = offset
                    .checked_add(compressed_len)
                    .ok_or_else(|| anyhow!("git pack offset overflow"))?;
                RawGitObject::Base { kind, data }
            }
            RawGitObjectKind::OffsetDelta => {
                let (negative_offset, used) = read_offset_delta_base(pack_bytes, offset, data_end)?;
                let base_offset = (entry_offset as u64)
                    .checked_sub(negative_offset)
                    .ok_or_else(|| anyhow!("git offset delta base offset underflow"))?;
                offset = offset
                    .checked_add(used)
                    .ok_or_else(|| anyhow!("git pack offset overflow"))?;
                let (delta, compressed_len) = inflate_object(&pack_bytes[offset..data_end])?;
                if delta.len() as u64 != size {
                    bail!("git pack delta inflated size mismatch");
                }
                offset = offset
                    .checked_add(compressed_len)
                    .ok_or_else(|| anyhow!("git pack offset overflow"))?;
                RawGitObject::OffsetDelta { base_offset, delta }
            }
            RawGitObjectKind::RefDelta => {
                let id_end = offset
                    .checked_add(20)
                    .ok_or_else(|| anyhow!("git pack ref-delta offset overflow"))?;
                if id_end > data_end {
                    bail!("git pack ref-delta base id is truncated");
                }
                let base_id = pack_bytes[offset..id_end].to_vec();
                offset = id_end;
                let (delta, compressed_len) = inflate_object(&pack_bytes[offset..data_end])?;
                if delta.len() as u64 != size {
                    bail!("git pack delta inflated size mismatch");
                }
                offset = offset
                    .checked_add(compressed_len)
                    .ok_or_else(|| anyhow!("git pack offset overflow"))?;
                RawGitObject::RefDelta { base_id, delta }
            }
        };
        raw_entries.push(RawGitPackEntry {
            entry_offset: entry_offset as u64,
            entry_len: (offset - entry_offset) as u64,
            object,
        });
    }
    if raw_entries.len() != expected_count || offset != data_end {
        bail!("git pack object count or trailing data mismatch");
    }
    resolve_pack_entries(raw_entries)
}

#[derive(Debug, Clone, Copy)]
enum RawGitObjectKind {
    Base(GitObjectKind),
    OffsetDelta,
    RefDelta,
}

fn read_object_header(
    pack_bytes: &[u8],
    offset: usize,
    data_end: usize,
) -> Result<(RawGitObjectKind, u64, usize)> {
    if offset >= data_end {
        bail!("git pack object header is truncated");
    }
    let first = pack_bytes[offset];
    let kind = match (first >> 4) & 0x07 {
        1 => RawGitObjectKind::Base(GitObjectKind::Commit),
        2 => RawGitObjectKind::Base(GitObjectKind::Tree),
        3 => RawGitObjectKind::Base(GitObjectKind::Blob),
        4 => RawGitObjectKind::Base(GitObjectKind::Tag),
        6 => RawGitObjectKind::OffsetDelta,
        7 => RawGitObjectKind::RefDelta,
        other => bail!("unsupported git pack object kind {other}"),
    };
    let mut size = u64::from(first & 0x0f);
    let mut shift = 4_u32;
    let mut used = 1_usize;
    let mut byte = first;
    while byte & 0x80 != 0 {
        let idx = offset
            .checked_add(used)
            .ok_or_else(|| anyhow!("git pack object header offset overflow"))?;
        if idx >= data_end {
            bail!("git pack object header continuation is truncated");
        }
        byte = pack_bytes[idx];
        size |= u64::from(byte & 0x7f) << shift;
        shift += 7;
        used += 1;
        if shift > 63 {
            bail!("git pack object size varint is too large");
        }
    }
    Ok((kind, size, used))
}

fn read_offset_delta_base(
    pack_bytes: &[u8],
    offset: usize,
    data_end: usize,
) -> Result<(u64, usize)> {
    if offset >= data_end {
        bail!("git offset-delta base offset is truncated");
    }
    let mut used = 1_usize;
    let mut byte = pack_bytes[offset];
    let mut value = u64::from(byte & 0x7f);
    while byte & 0x80 != 0 {
        let idx = offset
            .checked_add(used)
            .ok_or_else(|| anyhow!("git offset-delta base offset overflow"))?;
        if idx >= data_end {
            bail!("git offset-delta base offset continuation is truncated");
        }
        byte = pack_bytes[idx];
        value = value
            .checked_add(1)
            .and_then(|value| value.checked_shl(7))
            .map(|value| value | u64::from(byte & 0x7f))
            .ok_or_else(|| anyhow!("git offset-delta base offset is too large"))?;
        used += 1;
    }
    Ok((value, used))
}

fn inflate_object(input: &[u8]) -> Result<(Vec<u8>, usize)> {
    let mut decoder = ZlibDecoder::new(input);
    let mut data = Vec::new();
    decoder.read_to_end(&mut data)?;
    let consumed = usize::try_from(decoder.total_in())?;
    if consumed == 0 || consumed > input.len() {
        bail!("git pack zlib stream consumed invalid length");
    }
    Ok((data, consumed))
}

fn resolve_pack_entries(raw_entries: Vec<RawGitPackEntry>) -> Result<Vec<GitPackObject>> {
    let mut resolved_by_offset = BTreeMap::<u64, GitPackObject>::new();
    let mut resolved_by_id = BTreeMap::<Vec<u8>, GitPackObject>::new();
    let mut pending = Vec::new();
    for entry in raw_entries {
        match entry.object {
            RawGitObject::Base { kind, data } => {
                let object = resolved_object(kind, entry.entry_offset, entry.entry_len, data);
                resolved_by_id.insert(object.object_id.clone(), object.clone());
                resolved_by_offset.insert(entry.entry_offset, object);
            }
            RawGitObject::OffsetDelta { .. } | RawGitObject::RefDelta { .. } => pending.push(entry),
        }
    }

    while !pending.is_empty() {
        let mut next_pending = Vec::new();
        let mut resolved_count = 0;
        for entry in pending {
            let base = match &entry.object {
                RawGitObject::OffsetDelta { base_offset, .. } => {
                    resolved_by_offset.get(base_offset)
                }
                RawGitObject::RefDelta { base_id, .. } => resolved_by_id.get(base_id),
                RawGitObject::Base { .. } => unreachable!(),
            };
            let Some(base) = base else {
                next_pending.push(entry);
                continue;
            };
            let delta = match &entry.object {
                RawGitObject::OffsetDelta { delta, .. } | RawGitObject::RefDelta { delta, .. } => {
                    delta
                }
                RawGitObject::Base { .. } => unreachable!(),
            };
            let data = apply_git_delta(&base.data, delta)?;
            let object = resolved_object(base.kind, entry.entry_offset, entry.entry_len, data);
            resolved_by_id.insert(object.object_id.clone(), object.clone());
            resolved_by_offset.insert(entry.entry_offset, object);
            resolved_count += 1;
        }
        if resolved_count == 0 {
            bail!("git pack contains unresolved delta base references");
        }
        pending = next_pending;
    }

    Ok(resolved_by_offset.into_values().collect())
}

fn resolved_object(
    kind: GitObjectKind,
    entry_offset: u64,
    entry_len: u64,
    data: Vec<u8>,
) -> GitPackObject {
    let object_id = git_object_id(kind, &data);
    GitPackObject {
        kind,
        object_id,
        entry_offset,
        entry_len,
        data,
    }
}

fn apply_git_delta(base: &[u8], delta: &[u8]) -> Result<Vec<u8>> {
    let mut offset = 0;
    let source_size = read_delta_varint(delta, &mut offset)?;
    if source_size != base.len() as u64 {
        bail!("git delta source size mismatch");
    }
    let target_size = read_delta_varint(delta, &mut offset)?;
    let mut out = Vec::with_capacity(usize::try_from(target_size)?);
    while offset < delta.len() {
        let opcode = delta[offset];
        offset += 1;
        if opcode & 0x80 != 0 {
            let mut copy_offset = 0_usize;
            let mut copy_size = 0_usize;
            if opcode & 0x01 != 0 {
                copy_offset |= read_delta_byte(delta, &mut offset)?;
            }
            if opcode & 0x02 != 0 {
                copy_offset |= read_delta_byte(delta, &mut offset)? << 8;
            }
            if opcode & 0x04 != 0 {
                copy_offset |= read_delta_byte(delta, &mut offset)? << 16;
            }
            if opcode & 0x08 != 0 {
                copy_offset |= read_delta_byte(delta, &mut offset)? << 24;
            }
            if opcode & 0x10 != 0 {
                copy_size |= read_delta_byte(delta, &mut offset)?;
            }
            if opcode & 0x20 != 0 {
                copy_size |= read_delta_byte(delta, &mut offset)? << 8;
            }
            if opcode & 0x40 != 0 {
                copy_size |= read_delta_byte(delta, &mut offset)? << 16;
            }
            if copy_size == 0 {
                copy_size = 0x10000;
            }
            let end = copy_offset
                .checked_add(copy_size)
                .ok_or_else(|| anyhow!("git delta copy range overflow"))?;
            if end > base.len() {
                bail!("git delta copy range exceeds base object");
            }
            out.extend_from_slice(&base[copy_offset..end]);
        } else if opcode != 0 {
            let insert_len = usize::from(opcode);
            let end = offset
                .checked_add(insert_len)
                .ok_or_else(|| anyhow!("git delta insert range overflow"))?;
            if end > delta.len() {
                bail!("git delta insert range exceeds delta data");
            }
            out.extend_from_slice(&delta[offset..end]);
            offset = end;
        } else {
            bail!("git delta reserved opcode encountered");
        }
    }
    if out.len() as u64 != target_size {
        bail!("git delta target size mismatch");
    }
    Ok(out)
}

fn read_delta_varint(delta: &[u8], offset: &mut usize) -> Result<u64> {
    let mut shift = 0_u32;
    let mut value = 0_u64;
    loop {
        if *offset >= delta.len() {
            bail!("git delta varint is truncated");
        }
        let byte = delta[*offset];
        *offset += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift > 63 {
            bail!("git delta varint is too large");
        }
    }
}

fn read_delta_byte(delta: &[u8], offset: &mut usize) -> Result<usize> {
    if *offset >= delta.len() {
        bail!("git delta instruction is truncated");
    }
    let value = usize::from(delta[*offset]);
    *offset += 1;
    Ok(value)
}

fn git_object_id(kind: GitObjectKind, data: &[u8]) -> Vec<u8> {
    let mut hasher = Sha1::new();
    hasher.update(format!("{} {}\0", kind.name(), data.len()).as_bytes());
    hasher.update(data);
    hasher.finalize().to_vec()
}

fn commit_root_tree_id(data: &[u8]) -> Result<Vec<u8>> {
    let text = std::str::from_utf8(data)?;
    let Some(line) = text.lines().find(|line| line.starts_with("tree ")) else {
        bail!("git commit object has no root tree");
    };
    let tree = line.trim_start_matches("tree ");
    if tree.len() != 40 {
        bail!("git commit root tree must be a SHA-1 hex id");
    }
    Ok(hex::decode(tree)?)
}

#[allow(clippy::too_many_arguments)]
fn collect_tree_records(
    repository_id: &str,
    by_id: &BTreeMap<Vec<u8>, &GitPackObject>,
    visited_trees: &mut BTreeSet<Vec<u8>>,
    commit_id: &[u8],
    tree_id: &[u8],
    prefix: &str,
    pack_object_version_id: [u8; 16],
    records: &mut Vec<GitSourceRecord>,
) -> Result<()> {
    if !visited_trees.insert(tree_id.to_vec()) {
        bail!("git tree cycle detected");
    }
    let tree = by_id
        .get(tree_id)
        .ok_or_else(|| anyhow!("git tree object referenced by commit is missing from pack"))?;
    if tree.kind != GitObjectKind::Tree {
        bail!("git commit root tree points to a non-tree object");
    }
    for entry in parse_tree_entries(&tree.data)? {
        let path = if prefix.is_empty() {
            entry.name.clone()
        } else {
            format!("{prefix}/{}", entry.name)
        };
        match entry.mode.as_str() {
            "40000" | "040000" => collect_tree_records(
                repository_id,
                by_id,
                visited_trees,
                commit_id,
                &entry.object_id,
                &path,
                pack_object_version_id,
                records,
            )?,
            "160000" => {}
            _ => {
                let blob = by_id.get(&entry.object_id).ok_or_else(|| {
                    anyhow!("git blob object referenced by tree is missing from pack")
                })?;
                if blob.kind != GitObjectKind::Blob {
                    bail!("git tree entry points to a non-blob object");
                }
                records.push(GitSourceRecord::new(
                    GitHashAlgorithm::Sha1,
                    repository_id.as_bytes().to_vec(),
                    commit_id.to_vec(),
                    entry.object_id,
                    path.into_bytes(),
                    blob.entry_offset,
                    blob.entry_len,
                    pack_object_version_id,
                )?);
            }
        }
    }
    visited_trees.remove(tree_id);
    Ok(())
}

#[derive(Debug)]
struct TreeEntry {
    mode: String,
    name: String,
    object_id: Vec<u8>,
}

fn parse_tree_entries(data: &[u8]) -> Result<Vec<TreeEntry>> {
    let mut entries = Vec::new();
    let mut offset = 0;
    while offset < data.len() {
        let mode_end = data[offset..]
            .iter()
            .position(|byte| *byte == b' ')
            .ok_or_else(|| anyhow!("git tree entry mode is unterminated"))?
            + offset;
        let name_start = mode_end + 1;
        let name_end = data[name_start..]
            .iter()
            .position(|byte| *byte == 0)
            .ok_or_else(|| anyhow!("git tree entry name is unterminated"))?
            + name_start;
        let id_start = name_end + 1;
        let id_end = id_start + 20;
        if id_end > data.len() {
            bail!("git tree entry object id is truncated");
        }
        let mode = std::str::from_utf8(&data[offset..mode_end])?.to_string();
        let name = std::str::from_utf8(&data[name_start..name_end])?.to_string();
        if name.is_empty() || name.contains('/') || name == "." || name == ".." {
            bail!("git tree entry name is invalid");
        }
        entries.push(TreeEntry {
            mode,
            name,
            object_id: data[id_start..id_end].to_vec(),
        });
        offset = id_end;
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{Compression, write::ZlibEncoder};
    use std::io::Write;

    #[test]
    fn parses_minimal_sha1_pack_into_blob_records() {
        let pack = minimal_pack(false);
        let parsed = build_git_source_index_from_pack("repo-alpha", &pack, [3; 16]).unwrap();
        assert_eq!(parsed.hash_algorithm, GitHashAlgorithm::Sha1);
        assert_eq!(parsed.pack_hash, hash32(&pack));
        assert_eq!(parsed.records.len(), 1);
        let record = &parsed.records[0];
        assert_eq!(record.repository_id, b"repo-alpha".to_vec());
        assert_eq!(record.tree_path, b"README.md".to_vec());
        assert_eq!(record.pack_object_version_id, [3; 16]);
        assert!(record.blob_len > 0);
    }

    #[test]
    fn resolves_ref_delta_blob_records() {
        let pack = minimal_pack(true);
        let parsed = build_git_source_index_from_pack("repo-alpha", &pack, [4; 16]).unwrap();
        assert_eq!(parsed.records.len(), 1);
        assert_eq!(parsed.records[0].tree_path, b"README.md".to_vec());
    }

    fn minimal_pack(use_delta_blob: bool) -> Vec<u8> {
        let base_blob = if use_delta_blob {
            b"hello".to_vec()
        } else {
            b"hello\n".to_vec()
        };
        let target_blob = if use_delta_blob {
            b"hello world\n".to_vec()
        } else {
            base_blob.clone()
        };
        let target_blob_id = object_id(GitObjectKind::Blob, &target_blob);
        let mut tree = Vec::new();
        tree.extend_from_slice(b"100644 README.md\0");
        tree.extend_from_slice(&target_blob_id);
        let tree_id = object_id(GitObjectKind::Tree, &tree);
        let commit = format!(
            "tree {}\nauthor A <a@example.test> 0 +0000\ncommitter A <a@example.test> 0 +0000\n\ninitial\n",
            hex::encode(&tree_id)
        )
        .into_bytes();
        let mut objects = vec![
            (RawPackTestObject::Base(GitObjectKind::Commit, commit)),
            (RawPackTestObject::Base(GitObjectKind::Tree, tree)),
        ];
        if use_delta_blob {
            let base_blob_id = object_id(GitObjectKind::Blob, &base_blob);
            objects.push(RawPackTestObject::Base(GitObjectKind::Blob, base_blob));
            objects.push(RawPackTestObject::RefDelta(
                base_blob_id,
                append_delta(b"hello", b" world\n"),
            ));
        } else {
            objects.push(RawPackTestObject::Base(GitObjectKind::Blob, target_blob));
        }
        let mut pack = Vec::new();
        pack.extend_from_slice(b"PACK");
        pack.extend_from_slice(&2_u32.to_be_bytes());
        pack.extend_from_slice(&(objects.len() as u32).to_be_bytes());
        for object in objects {
            write_pack_object(&mut pack, object);
        }
        let mut hasher = Sha1::new();
        hasher.update(&pack);
        pack.extend_from_slice(&hasher.finalize());
        pack
    }

    enum RawPackTestObject {
        Base(GitObjectKind, Vec<u8>),
        RefDelta(Vec<u8>, Vec<u8>),
    }

    fn write_pack_object(pack: &mut Vec<u8>, object: RawPackTestObject) {
        match object {
            RawPackTestObject::Base(kind, data) => {
                let kind_bits = match kind {
                    GitObjectKind::Commit => 1,
                    GitObjectKind::Tree => 2,
                    GitObjectKind::Blob => 3,
                    GitObjectKind::Tag => 4,
                };
                write_header(pack, kind_bits, data.len());
                write_zlib(pack, &data);
            }
            RawPackTestObject::RefDelta(base_id, delta) => {
                write_header(pack, 7, delta.len());
                pack.extend_from_slice(&base_id);
                write_zlib(pack, &delta);
            }
        }
    }

    fn write_header(pack: &mut Vec<u8>, kind_bits: u8, len: usize) {
        let mut size = len as u64;
        let mut first = (kind_bits << 4) | ((size as u8) & 0x0f);
        size >>= 4;
        if size != 0 {
            first |= 0x80;
        }
        pack.push(first);
        while size != 0 {
            let mut byte = (size as u8) & 0x7f;
            size >>= 7;
            if size != 0 {
                byte |= 0x80;
            }
            pack.push(byte);
        }
    }

    fn write_zlib(pack: &mut Vec<u8>, data: &[u8]) {
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data).unwrap();
        pack.extend_from_slice(&encoder.finish().unwrap());
    }

    fn append_delta(base: &[u8], suffix: &[u8]) -> Vec<u8> {
        let mut delta = Vec::new();
        write_delta_varint(&mut delta, base.len() as u64);
        write_delta_varint(&mut delta, (base.len() + suffix.len()) as u64);
        delta.push(0x90);
        delta.push(base.len() as u8);
        delta.push(suffix.len() as u8);
        delta.extend_from_slice(suffix);
        delta
    }

    fn write_delta_varint(out: &mut Vec<u8>, mut value: u64) {
        loop {
            let mut byte = (value as u8) & 0x7f;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            out.push(byte);
            if value == 0 {
                break;
            }
        }
    }

    fn object_id(kind: GitObjectKind, data: &[u8]) -> Vec<u8> {
        git_object_id(kind, data)
    }
}
