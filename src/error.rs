use thiserror::Error;

/// Represents the possible errors that can occur in the FIX router
#[derive(Error, Debug)]
pub enum FixRouterError {
    /// Error when parsing a FIX message
    #[error("Failed to parse FIX message: {0}")]
    ParseError(String),

    /// Error when a required tag is missing
    #[error("Required tag {0} missing")]
    MissingTag(String),

    /// Error when routing a message
    #[error("Routing error: {0}")]
    RoutingError(String),

    /// Error when adding or removing nodes
    #[error("Node operation error: {0}")]
    NodeOperationError(String),

    /// Error when performing hashing operations
    #[error("Hashing error: {0}")]
    HashingError(String),
}