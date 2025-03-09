use std::collections::HashMap;
use crate::error::FixRouterError;

/// Represents a parsed FIX message
#[derive(Debug)]
pub struct FixMessage {
    /// All tags and their values from the FIX message
    pub tags: HashMap<String, String>,
    /// Message type (tag 35)
    pub msg_type: String,
    /// Sender CompID (tag 49) 
    pub sender: String,
    /// Target CompID (tag 56)
    pub target: String,
    /// Message sequence number (tag 34)
    pub seq_num: String,
    /// ClOrdID if present (tag 11)
    pub cl_ord_id: Option<String>,
    /// OrderID if present (tag 37)
    pub order_id: Option<String>,
}

impl FixMessage {
    /// Creates a new empty FIX message
    pub fn new() -> Self {
        FixMessage {
            tags: HashMap::new(),
            msg_type: String::new(),
            sender: String::new(),
            target: String::new(),
            seq_num: String::new(),
            cl_ord_id: None,
            order_id: None,
        }
    }
    
    /// Gets a routing key for the message based on business logic
    /// 
    /// In FIX routing, we typically want to maintain order of messages for a specific
    /// instrument or order. This function returns a routing key based on one of:
    /// 1. OrderID (tag 37) if present
    /// 2. ClOrdID (tag 11) if present
    /// 3. A combination of SenderCompID + TargetCompID + MsgSeqNum if no order identifiers exist
    pub fn get_routing_key(&self) -> String {
        if let Some(ref order_id) = self.order_id {
            // Use OrderID as routing key for existing orders
            return format!("order:{}", order_id);
        } else if let Some(ref cl_ord_id) = self.cl_ord_id {
            // Use ClOrdID for new orders
            return format!("clord:{}", cl_ord_id);
        } else {
            // Default to sender+target+sequence for messages without order references
            return format!("{}:{}:{}", self.sender, self.target, self.seq_num);
        }
    }
}

/// Parses a FIX message string into a structured FixMessage object
///
/// # Arguments
/// * `message` - The FIX message string, with fields separated by the delimiter
/// * `delimiter` - The field delimiter character (traditionally '|' in this implementation)
///
/// # Returns
/// Result containing the parsed FixMessage or an error
pub fn parse_fix_message(message: &str, delimiter: char) -> Result<FixMessage, FixRouterError> {
    let mut fix_msg = FixMessage::new();
    
    for field in message.split(delimiter) {
        if field.is_empty() {
            continue;
        }
        
        // FIX fields are in format tag=value
        let parts: Vec<&str> = field.split('=').collect();
        if parts.len() != 2 {
            continue; // Skip malformed fields
        }
        
        let tag = parts[0];
        let value = parts[1];
        
        // Store all tags in the HashMap
        fix_msg.tags.insert(tag.to_string(), value.to_string());
        
        // Populate specific fields for routing
        match tag {
            "35" => fix_msg.msg_type = value.to_string(),
            "49" => fix_msg.sender = value.to_string(),
            "56" => fix_msg.target = value.to_string(),
            "34" => fix_msg.seq_num = value.to_string(),
            "11" => fix_msg.cl_ord_id = Some(value.to_string()),
            "37" => fix_msg.order_id = Some(value.to_string()),
            _ => {} // Ignore other tags
        }
    }
    
    // Validate minimum required fields for routing
    if fix_msg.msg_type.is_empty() {
        return Err(FixRouterError::MissingTag("35 (MsgType)".to_string()));
    }
    if fix_msg.sender.is_empty() {
        return Err(FixRouterError::MissingTag("49 (SenderCompID)".to_string()));
    }
    if fix_msg.target.is_empty() {
        return Err(FixRouterError::MissingTag("56 (TargetCompID)".to_string()));
    }
    if fix_msg.seq_num.is_empty() {
        return Err(FixRouterError::MissingTag("34 (MsgSeqNum)".to_string()));
    }
    
    Ok(fix_msg)
}