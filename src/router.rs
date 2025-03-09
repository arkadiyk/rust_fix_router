use crate::consistent_hash::ConsistentHashRing;
use crate::fix_parser::{parse_fix_message, FixMessage};
use crate::error::FixRouterError;

/// Main FIX Router that distributes messages to nodes using consistent hashing
#[derive(Clone)]
pub struct FixRouter {
    /// The consistent hash ring that manages node distribution
    hash_ring: ConsistentHashRing,
    /// The delimiter used in FIX messages
    delimiter: char,
}

impl FixRouter {
    /// Creates a new FIX router with the specified number of virtual nodes
    ///
    /// # Arguments
    /// * `virtual_nodes` - Number of virtual nodes per physical node
    ///
    /// # Returns
    /// A new FixRouter instance
    pub fn new(virtual_nodes: usize) -> Self {
        FixRouter {
            hash_ring: ConsistentHashRing::new(virtual_nodes),
            delimiter: '|', // Default delimiter for FIX messages in our implementation
        }
    }

    /// Adds a node to the router's hash ring
    ///
    /// # Arguments
    /// * `node_id` - Unique identifier for the node
    /// * `address` - Network address of the node
    ///
    /// # Returns
    /// Result indicating success or failure
    pub fn add_node(&mut self, node_id: &str, address: &str) -> Result<(), FixRouterError> {
        self.hash_ring.add_node(node_id, address)
    }

    /// Removes a node from the router's hash ring
    ///
    /// # Arguments
    /// * `node_id` - Identifier of the node to remove
    ///
    /// # Returns
    /// Result indicating success or failure
    pub fn remove_node(&mut self, node_id: &str) -> Result<(), FixRouterError> {
        self.hash_ring.remove_node(node_id)
    }

    /// Routes a FIX message to the appropriate node
    ///
    /// # Business Logic for Routing
    /// - Parses the FIX message to extract routing information
    /// - Determines a routing key based on order identifiers to ensure related messages
    ///   go to the same node (session affinity)
    /// - Uses consistent hashing to map the routing key to a specific node
    ///
    /// # Arguments
    /// * `message` - The FIX message string to route
    ///
    /// # Returns
    /// Result containing the address of the selected node or an error
    pub fn route_message(&self, message: &str) -> Result<String, FixRouterError> {
        // Parse the FIX message
        let fix_msg = parse_fix_message(message, self.delimiter)?;
        
        // Get the routing key based on business logic
        let routing_key = fix_msg.get_routing_key();
        
        // For debugging purposes in tests
        println!("Routing key: {}", routing_key);
        
        // Use consistent hashing to determine the target node
        let node = self.hash_ring.get_node(&routing_key)?;
        
        // For debugging purposes in tests
        println!("Selected node: {}", node);
        
        Ok(node)
    }
    
    /// Routes a batch of FIX messages and returns their destinations
    ///
    /// # Arguments
    /// * `messages` - A vector of FIX message strings
    ///
    /// # Returns
    /// A vector of Results, each containing either a node address or an error
    pub fn route_batch(&self, messages: Vec<&str>) -> Vec<Result<String, FixRouterError>> {
        messages.iter().map(|msg| self.route_message(msg)).collect()
    }
    
    /// Gets information about the current state of the router
    ///
    /// # Returns
    /// A tuple containing (number of physical nodes, number of virtual nodes)
    pub fn get_stats(&self) -> (usize, usize) {
        (self.hash_ring.node_count(), self.hash_ring.virtual_node_count())
    }
    
    /// Gets a list of all node IDs in the router
    ///
    /// # Returns
    /// A vector of node ID strings
    pub fn get_all_nodes(&self) -> Vec<String> {
        self.hash_ring.get_all_nodes()
    }
    
    /// Changes the delimiter used for parsing FIX messages
    ///
    /// # Arguments
    /// * `delimiter` - The new delimiter character
    pub fn set_delimiter(&mut self, delimiter: char) {
        self.delimiter = delimiter;
    }
}