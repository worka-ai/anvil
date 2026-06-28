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
    let mut objects = Vec::with_capacity(expected_count);
    for _ in 0..expected_count {
        let entry_offset = offset;
        let (kind, size, header_len) = read_object_header(pack_bytes, offset, data_end)?;
        offset = offset
            .checked_add(header_len)
            .ok_or_else(|| anyhow!("git pack offset overflow"))?;
        let (data, compressed_len) = inflate_object(&pack_bytes[offset..data_end])?;
        if data.len() as u64 != size {
            bail!("git pack object inflated size mismatch");
        }
        offset = offset
            .checked_add(compressed_len)
            .ok_or_else(|| anyhow!("git pack offset overflow"))?;
        let object_id = git_object_id(kind, &data);
        objects.push(GitPackObject {
            kind,
            object_id,
            entry_offset: entry_offset as u64,
            entry_len: (offset - entry_offset) as u64,
            data,
        });
    }
    if objects.len() != expected_count || offset != data_end {
        bail!("git pack object count or trailing data mismatch");
    }
    Ok(objects)
}

fn read_object_header(
    pack_bytes: &[u8],
    offset: usize,
    data_end: usize,
) -> Result<(GitObjectKind, u64, usize)> {
    if offset >= data_end {
        bail!("git pack object header is truncated");
    }
    let first = pack_bytes[offset];
    let kind = match (first >> 4) & 0x07 {
        1 => GitObjectKind::Commit,
        2 => GitObjectKind::Tree,
        3 => GitObjectKind::Blob,
        4 => GitObjectKind::Tag,
        6 | 7 => bail!("git pack delta objects are not yet supported by source indexing"),
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
        let pack = minimal_pack();
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

    fn minimal_pack() -> Vec<u8> {
        let blob = b"hello\n".to_vec();
        let blob_id = object_id(GitObjectKind::Blob, &blob);
        let mut tree = Vec::new();
        tree.extend_from_slice(b"100644 README.md\0");
        tree.extend_from_slice(&blob_id);
        let tree_id = object_id(GitObjectKind::Tree, &tree);
        let commit = format!(
            "tree {}\nauthor A <a@example.test> 0 +0000\ncommitter A <a@example.test> 0 +0000\n\ninitial\n",
            hex::encode(&tree_id)
        )
        .into_bytes();
        let objects = vec![
            (GitObjectKind::Commit, commit),
            (GitObjectKind::Tree, tree),
            (GitObjectKind::Blob, blob),
        ];
        let mut pack = Vec::new();
        pack.extend_from_slice(b"PACK");
        pack.extend_from_slice(&2_u32.to_be_bytes());
        pack.extend_from_slice(&(objects.len() as u32).to_be_bytes());
        for (kind, data) in objects {
            write_pack_object(&mut pack, kind, &data);
        }
        let mut hasher = Sha1::new();
        hasher.update(&pack);
        pack.extend_from_slice(&hasher.finalize());
        pack
    }

    fn write_pack_object(pack: &mut Vec<u8>, kind: GitObjectKind, data: &[u8]) {
        let kind_bits = match kind {
            GitObjectKind::Commit => 1,
            GitObjectKind::Tree => 2,
            GitObjectKind::Blob => 3,
            GitObjectKind::Tag => 4,
        };
        let mut size = data.len() as u64;
        let mut first = ((kind_bits as u8) << 4) | ((size as u8) & 0x0f);
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
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data).unwrap();
        pack.extend_from_slice(&encoder.finish().unwrap());
    }

    fn object_id(kind: GitObjectKind, data: &[u8]) -> Vec<u8> {
        git_object_id(kind, data)
    }
}
