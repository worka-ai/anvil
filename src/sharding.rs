use anyhow::Result;
use reed_solomon_erasure::galois_8::Field;
use reed_solomon_erasure::{Error, ReedSolomon};

// Define our sharding configuration.
// For now, we'll use a fixed 4+2 configuration (4 data shards, 2 parity shards).
// This means we can lose any 2 shards and still reconstruct the data.
const DATA_SHARDS: usize = 4;
const PARITY_SHARDS: usize = 2;

#[derive(Clone)]
pub struct ShardManager {
    codec: ReedSolomon<Field>,
}

impl ShardManager {
    pub fn new() -> Self {
        let codec = ReedSolomon::new(DATA_SHARDS, PARITY_SHARDS).unwrap();
        Self { codec }
    }

    pub fn new_with_config(data_shards: usize, parity_shards: usize) -> Self {
        let codec = ReedSolomon::new(data_shards, parity_shards).unwrap();
        Self { codec }
    }

    /// Encodes a single data stripe into data + parity shards.
    pub fn encode(&self, stripe: &mut [Vec<u8>]) -> Result<(), Error> {
        self.codec.encode(stripe)
    }

    /// Reconstructs a data stripe from a set of shards.
    /// `shards` is a slice of `Option<Vec<u8>>`, where `None` represents a missing shard.
    pub fn reconstruct(&self, shards: &mut [Option<Vec<u8>>]) -> Result<(), Error> {
        self.codec.reconstruct(shards)
    }

    pub fn data_shards(&self) -> usize {
        DATA_SHARDS
    }

    pub fn total_shards(&self) -> usize {
        DATA_SHARDS + PARITY_SHARDS
    }
}

impl Default for ShardManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_and_reconstruct() {
        let manager = ShardManager::new();
        let stripe_size = 1024;
        let mut data = vec![vec![0; stripe_size]; manager.total_shards()];

        // Fill the data shards with some data
        for i in 0..manager.data_shards() {
            for (j, byte) in data[i].iter_mut().enumerate() {
                *byte = (i * stripe_size + j) as u8;
            }
        }

        // Encode the data to generate parity shards
        manager.encode(&mut data).unwrap();

        // "Lose" two shards (one data, one parity)
        let mut shards: Vec<Option<Vec<u8>>> = data.into_iter().map(Some).collect();
        shards[0] = None; // Lose the first data shard
        shards[5] = None; // Lose the second parity shard

        // Reconstruct the data
        manager.reconstruct(&mut shards).unwrap();

        // Verify that the lost data shard was reconstructed correctly
        let reconstructed_shard = shards[0].as_ref().unwrap();
        for (j, byte) in reconstructed_shard.iter().enumerate() {
            assert_eq!(*byte, j as u8, "Reconstructed data does not match");
        }
    }
}
