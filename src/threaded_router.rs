use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use crate::router::FixRouter;
use crate::error::FixRouterError;

/// Represents a message along with its result callback
struct MessageTask {
    /// The FIX message to be processed
    message: String,
    /// Callback sender for the routing result
    result_sender: oneshot::Sender<Result<String, FixRouterError>>,
}

/// A threaded FIX router that processes messages asynchronously
pub struct ThreadedFixRouter {
    /// Channel for sending messages to worker tasks
    task_sender: mpsc::Sender<MessageTask>,
    /// Flag to signal workers to shut down
    shutdown: Arc<AtomicBool>,
    /// Handles to the worker tasks
    worker_handles: Vec<JoinHandle<()>>,
}

impl ThreadedFixRouter {
    /// Creates a new threaded router with the specified number of worker tasks
    ///
    /// # Arguments
    /// * `router` - The underlying FixRouter to use for message routing
    /// * `num_threads` - Number of worker tasks to create (defaults to number of CPU cores if None)
    ///
    /// # Returns
    /// A new ThreadedFixRouter instance
    pub fn new(router: FixRouter, num_threads: Option<usize>) -> Self {
        let thread_count = num_threads.unwrap_or_else(num_cpus::get);
        let (task_sender, mut task_receiver) = mpsc::channel(1000); // Buffer up to 1000 messages
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut worker_handles = Vec::with_capacity(thread_count);
        
        // Wrap router in an Arc for safe sharing across tasks
        let shared_router = Arc::new(router);
        
        // Create a channel for each worker
        let mut worker_senders = Vec::with_capacity(thread_count);
        for _ in 0..thread_count {
            let (tx, rx) = mpsc::channel(100);
            worker_senders.push(tx);
            
            let router_clone = Arc::clone(&shared_router);
            let shutdown_clone = Arc::clone(&shutdown);
            
            // Create a worker task
            let handle = tokio::spawn(async move {
                // Process messages in this worker's loop
                Self::worker_loop(router_clone, rx, shutdown_clone).await;
            });
            
            worker_handles.push(handle);
        }
        
        // Spawn a task to distribute messages round-robin to workers
        let shutdown_clone = Arc::clone(&shutdown);
        tokio::spawn(async move {
            let mut next_worker = 0;
            while !shutdown_clone.load(Ordering::Relaxed) {
                if let Some(task) = task_receiver.recv().await {
                    if let Some(sender) = worker_senders.get(next_worker) {
                        if sender.send(task).await.is_err() {
                            break;
                        }
                    }
                    next_worker = (next_worker + 1) % thread_count;
                } else {
                    break;
                }
            }
        });
        
        ThreadedFixRouter {
            task_sender,
            shutdown,
            worker_handles,
        }
    }
    
    /// The main loop for worker tasks
    async fn worker_loop(
        router: Arc<FixRouter>, 
        mut receiver: mpsc::Receiver<MessageTask>, 
        shutdown: Arc<AtomicBool>
    ) {
        while !shutdown.load(Ordering::Relaxed) {
            match receiver.recv().await {
                Some(task) => {
                    // Process the message
                    let result = router.route_message(&task.message);
                    
                    // Send the result back through the callback channel
                    let _ = task.result_sender.send(result);
                },
                None => {
                    // Channel closed, exit the loop
                    break;
                }
            }
        }
    }
    
    /// Routes a FIX message asynchronously
    ///
    /// # Arguments
    /// * `message` - The FIX message to route
    ///
    /// # Returns
    /// A oneshot receiver for the routing result
    pub async fn route_message_async(&self, message: &str) -> Result<String, FixRouterError> {
        let (result_sender, result_receiver) = oneshot::channel();
        
        let task = MessageTask {
            message: message.to_string(),
            result_sender,
        };
        
        // Send the task to a worker task
        self.task_sender.send(task).await
            .expect("Failed to send task to worker task");
        
        // Wait for and return the result
        result_receiver.await
            .expect("Worker task failed to send result")
    }
    
    /// Shuts down all worker tasks gracefully
    pub async fn shutdown(self) {
        // Signal tasks to shut down
        self.shutdown.store(true, Ordering::Relaxed);
        
        // Wait for all tasks to finish
        for handle in self.worker_handles {
            let _ = handle.await;
        }
    }
    
    /// Routes a batch of FIX messages asynchronously
    ///
    /// # Arguments
    /// * `messages` - A vector of FIX message strings
    ///
    /// # Returns
    /// A vector of Results, each containing either a node address or an error
    pub async fn route_batch_async(&self, messages: &[&str]) -> Vec<Result<String, FixRouterError>> {
        let mut results = Vec::with_capacity(messages.len());
        
        for message in messages {
            results.push(self.route_message_async(message).await);
        }
        
        results
    }
}