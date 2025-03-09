use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use md5;

use crate::error::FixRouterError;

/// A consistent hash ring implementation that manages the distribution of nodes
pub struct ConsistentHashRing {
    /// The number of virtual nodes to create per physical node
    virtual_nodes: usize,
    /// A map of hash positions to node identifiers
    ring: BTreeMap<u64, String>,
    /// A map of node identifiers to their addresses
    nodes: HashMap<String, String>,
}

impl ConsistentHashRing {
    /// Creates a new consistent hash ring with the specified number of virtual nodes per physical node
    ///
    /// # Arguments
    /// * `virtual_nodes` - The number of virtual replicas per physical node
    ///
    /// # Returns
    /// A new ConsistentHashRing instance
    pub fn new(virtual_nodes: usize) -> Self {
        ConsistentHashRing {
            virtual_nodes,
            ring: BTreeMap::new(),
            nodes: HashMap::new(),
        }
    }

    /// Adds a node to the hash ring
    ///
    /// # Arguments
    /// * `node_id` - Unique identifier for the node
    /// * `address` - Network address for the node (e.g., "192.168.1.1:9000")
    ///
    /// # Returns
    /// Result indicating success or failure
    pub fn add_node(&mut self, node_id: &str, address: &str) -> Result<(), FixRouterError> {
        if self.nodes.contains_key(node_id) {
            return Err(FixRouterError::NodeOperationError(
                format!("Node {} already exists", node_id)
            ));
        }

        self.nodes.insert(node_id.to_string(), address.to_string());

        // Add virtual nodes to the ring
        for i in 0..self.virtual_nodes {
            let virtual_node_id = format!("{}:{}", node_id, i);
            let hash = self.get_hash(&virtual_node_id)?;
            self.ring.insert(hash, node_id.to_string());
        }

        Ok(())
    }

    /// Removes a node from the hash ring
    ///
    /// # Arguments
    /// * `node_id` - Unique identifier for the node to remove
    ///
    /// # Returns
    /// Result indicating success or failure
    pub fn remove_node(&mut self, node_id: &str) -> Result<(), FixRouterError> {
        if !self.nodes.contains_key(node_id) {
            return Err(FixRouterError::NodeOperationError(
                format!("Node {} does not exist", node_id)
            ));
        }

        self.nodes.remove(node_id);

        // Remove virtual nodes from the ring
        let mut keys_to_remove = Vec::new();
        for (hash, id) in &self.ring {
            if id == node_id {
                keys_to_remove.push(*hash);
            }
        }

        for hash in keys_to_remove {
            self.ring.remove(&hash);
        }

        Ok(())
    }

    /// Gets the node responsible for a key
    ///
    /// # Arguments
    /// * `key` - The key to look up
    ///
    /// # Returns
    /// Result containing the address of the node or an error
    pub fn get_node(&self, key: &str) -> Result<String, FixRouterError> {
        if self.ring.is_empty() {
            return Err(FixRouterError::RoutingError("Hash ring is empty".into()));
        }

        let hash = self.get_hash(key)?;
        
        // Find the first node with a hash >= key's hash (or wrap around)
        if let Some((_, node_id)) = self.ring.range(hash..).next().or_else(|| self.ring.iter().next()) {
            if let Some(address) = self.nodes.get(node_id) {
                return Ok(address.clone());
            }
        }

        Err(FixRouterError::RoutingError("No node available".into()))
    }

    /// Calculates the hash value for a key
    ///
    /// # Arguments
    /// * `key` - The string key to hash
    ///
    /// # Returns
    /// Result containing the u64 hash value or an error
    fn get_hash(&self, key: &str) -> Result<u64, FixRouterError> {
        // Use MD5 for better distribution
        let digest = md5::compute(key.as_bytes());
        
        // Convert first 8 bytes of the MD5 hash to a u64
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&digest.0[0..8]);
        
        Ok(u64::from_ne_bytes(bytes))
    }

    /// Gets the number of nodes in the ring
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Gets the number of virtual nodes in the ring
    pub fn virtual_node_count(&self) -> usize {
        self.ring.len()
    }

    /// Gets a list of all physical node IDs
    pub fn get_all_nodes(&self) -> Vec<String> {
        self.nodes.keys().cloned().collect()
    }
}