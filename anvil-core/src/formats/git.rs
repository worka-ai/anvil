use super::{FormatError, Hash32, hash32};
use std::convert::TryInto;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GitHashAlgorithm {
    Sha1,
    Sha256,
}

impl GitHashAlgorithm {
    pub fn object_id_len(self) -> usize {
        match self {
            Self::Sha1 => 20,
            Self::Sha256 => 32,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitSourceRecord {
    pub repository_id: Vec<u8>,
    pub commit_id: Vec<u8>,
    pub object_id: Vec<u8>,
    pub tree_path: Vec<u8>,
    pub blob_start: u64,
    pub blob_len: u64,
    pub pack_object_version_id: [u8; 16],
    pub record_hash: Hash32,
}

impl GitSourceRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        hash_algorithm: GitHashAlgorithm,
        repository_id: Vec<u8>,
        commit_id: Vec<u8>,
        object_id: Vec<u8>,
        tree_path: Vec<u8>,
        blob_start: u64,
        blob_len: u64,
        pack_object_version_id: [u8; 16],
    ) -> Result<Self, FormatError> {
        validate_git_id_len(hash_algorithm, "git commit id", commit_id.len())?;
        validate_git_id_len(hash_algorithm, "git object id", object_id.len())?;
        let mut record = Self {
            repository_id,
            commit_id,
            object_id,
            tree_path,
            blob_start,
            blob_len,
            pack_object_version_id,
            record_hash: [0; 32],
        };
        record.record_hash = hash32(&record.bytes_without_hash());
        Ok(record)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = self.bytes_without_hash();
        out.extend_from_slice(&self.record_hash);
        out
    }

    pub fn decode(
        input: &[u8],
        hash_algorithm: GitHashAlgorithm,
    ) -> Result<(Self, usize), FormatError> {
        let id_len = hash_algorithm.object_id_len();
        let fixed_len = 2 + id_len + id_len + 2 + 8 + 8 + 16;
        if input.len() < fixed_len + 32 {
            return Err(FormatError::TooShort {
                context: "git source record",
                needed: fixed_len + 32,
                actual: input.len(),
            });
        }
        let repository_id_len = u16::from_le_bytes(input[0..2].try_into().unwrap()) as usize;
        let commit_id_start = 2;
        let object_id_start = commit_id_start + id_len;
        let tree_path_len_offset = object_id_start + id_len;
        let tree_path_len = u16::from_le_bytes(
            input[tree_path_len_offset..tree_path_len_offset + 2]
                .try_into()
                .unwrap(),
        ) as usize;
        let blob_start_offset = tree_path_len_offset + 2;
        let blob_len_offset = blob_start_offset + 8;
        let pack_object_version_id_offset = blob_len_offset + 8;
        let repository_id_start = pack_object_version_id_offset + 16;
        let tree_path_start = repository_id_start.checked_add(repository_id_len).ok_or(
            FormatError::InvalidDeclaredLength {
                context: "git repository id",
            },
        )?;
        let hash_start = tree_path_start.checked_add(tree_path_len).ok_or(
            FormatError::InvalidDeclaredLength {
                context: "git tree path",
            },
        )?;
        let record_end = hash_start
            .checked_add(32)
            .ok_or(FormatError::InvalidDeclaredLength {
                context: "git record hash",
            })?;
        if input.len() < record_end {
            return Err(FormatError::TooShort {
                context: "git source record bytes",
                needed: record_end,
                actual: input.len(),
            });
        }
        let record_hash = input[hash_start..record_end].try_into().unwrap();
        if hash32(&input[..hash_start]) != record_hash {
            return Err(FormatError::HashMismatch {
                context: "git source record",
            });
        }
        Ok((
            Self {
                repository_id: input[repository_id_start..tree_path_start].to_vec(),
                commit_id: input[commit_id_start..object_id_start].to_vec(),
                object_id: input[object_id_start..tree_path_len_offset].to_vec(),
                tree_path: input[tree_path_start..hash_start].to_vec(),
                blob_start: u64::from_le_bytes(
                    input[blob_start_offset..blob_start_offset + 8]
                        .try_into()
                        .unwrap(),
                ),
                blob_len: u64::from_le_bytes(
                    input[blob_len_offset..blob_len_offset + 8]
                        .try_into()
                        .unwrap(),
                ),
                pack_object_version_id: input
                    [pack_object_version_id_offset..pack_object_version_id_offset + 16]
                    .try_into()
                    .unwrap(),
                record_hash,
            },
            record_end,
        ))
    }

    fn bytes_without_hash(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(
            2 + self.commit_id.len()
                + self.object_id.len()
                + 2
                + 8
                + 8
                + 16
                + self.repository_id.len()
                + self.tree_path.len(),
        );
        out.extend_from_slice(&(self.repository_id.len() as u16).to_le_bytes());
        out.extend_from_slice(&self.commit_id);
        out.extend_from_slice(&self.object_id);
        out.extend_from_slice(&(self.tree_path.len() as u16).to_le_bytes());
        out.extend_from_slice(&self.blob_start.to_le_bytes());
        out.extend_from_slice(&self.blob_len.to_le_bytes());
        out.extend_from_slice(&self.pack_object_version_id);
        out.extend_from_slice(&self.repository_id);
        out.extend_from_slice(&self.tree_path);
        out
    }
}

fn validate_git_id_len(
    hash_algorithm: GitHashAlgorithm,
    context: &'static str,
    actual: usize,
) -> Result<(), FormatError> {
    let expected = hash_algorithm.object_id_len();
    if actual != expected {
        return Err(FormatError::InvalidFixedLength {
            context,
            expected,
            actual,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn git_source_record_round_trip_for_sha1() {
        let record = GitSourceRecord::new(
            GitHashAlgorithm::Sha1,
            b"repo".to_vec(),
            vec![1; 20],
            vec![2; 20],
            b"src/lib.rs".to_vec(),
            10,
            200,
            [3; 16],
        )
        .unwrap();
        let encoded = record.encode();
        let (decoded, used) = GitSourceRecord::decode(&encoded, GitHashAlgorithm::Sha1).unwrap();
        assert_eq!(used, encoded.len());
        assert_eq!(decoded, record);
    }

    #[test]
    fn git_source_record_rejects_wrong_hash_algorithm_length() {
        let err = GitSourceRecord::new(
            GitHashAlgorithm::Sha256,
            b"repo".to_vec(),
            vec![1; 20],
            vec![2; 32],
            b"README.md".to_vec(),
            0,
            1,
            [4; 16],
        )
        .unwrap_err();
        assert_eq!(
            err,
            FormatError::InvalidFixedLength {
                context: "git commit id",
                expected: 32,
                actual: 20
            }
        );
    }
}
