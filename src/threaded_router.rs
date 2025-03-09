use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::thread;
use crossbeam_channel::{bounded, Sender, Receiver};
use crate::router::FixRouter;
use crate::error::FixRouterError;
use crate::fix_parser;

/// Represents a message along with its result callback
struct MessageTask {
    /// The FIX message to be processed
    message: String,
    /// Callback sender for the routing result
    result_sender: Sender<Result<String, FixRouterError>>,
}

/// A threaded FIX router that processes messages asynchronously
pub struct ThreadedFixRouter {
    /// Router shared across all threads (wrapped in Arc for thread-safe access)
    router: Arc<FixRouter>,
    /// Channel for sending messages to worker threads
    task_sender: Sender<MessageTask>,
    /// Flag to signal workers to shut down
    shutdown: Arc<AtomicBool>,
    /// Handles to the worker threads
    worker_handles: Vec<thread::JoinHandle<()>>,
}

impl ThreadedFixRouter {
    /// Creates a new threaded router with the specified number of worker threads
    ///
    /// # Arguments
    /// * `router` - The underlying FixRouter to use for message routing
    /// * `num_threads` - Number of worker threads to create (defaults to number of CPU cores if None)
    ///
    /// # Returns
    /// A new ThreadedFixRouter instance
    pub fn new(router: FixRouter, num_threads: Option<usize>) -> Self {
        let thread_count = num_threads.unwrap_or_else(num_cpus::get);
        let (task_sender, task_receiver) = bounded(1000); // Buffer up to 1000 messages
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut worker_handles = Vec::with_capacity(thread_count);
        
        // Wrap router in an Arc for safe sharing across threads
        let shared_router = Arc::new(router);
        
        // Create the worker threads
        for _ in 0..thread_count {
            let router_clone = Arc::clone(&shared_router);
            let task_receiver_clone = task_receiver.clone();
            let shutdown_clone = Arc::clone(&shutdown);
            
            let handle = thread::spawn(move || {
                Self::worker_loop(router_clone, task_receiver_clone, shutdown_clone);
            });
            
            worker_handles.push(handle);
        }
        
        // Drop the last receiver so the channel will close when the ThreadedFixRouter is dropped
        drop(task_receiver);
        
        ThreadedFixRouter {
            router: shared_router,
            task_sender,
            shutdown,
            worker_handles,
        }
    }
    
    /// The main loop for worker threads
    fn worker_loop(router: Arc<FixRouter>, receiver: Receiver<MessageTask>, shutdown: Arc<AtomicBool>) {
        while !shutdown.load(Ordering::Relaxed) {
            match receiver.recv() {
                Ok(task) => {
                    // Process the message
                    let result = router.route_message(&task.message);
                    
                    // Send the result back through the callback channel
                    let _ = task.result_sender.send(result);
                },
                Err(_) => {
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
    /// A receiver for the routing result
    pub fn route_message_async(&self, message: &str) -> Receiver<Result<String, FixRouterError>> {
        let (result_sender, result_receiver) = bounded(1);
        
        let task = MessageTask {
            message: message.to_string(),
            result_sender,
        };
        
        // Send the task to a worker thread
        self.task_sender.send(task).expect("Failed to send task to worker thread");
        
        result_receiver
    }
    
    /// Routes a FIX message synchronously, useful for testing consistency
    ///
    /// # Arguments
    /// * `message` - The FIX message to route
    ///
    /// # Returns
    /// Result containing the node address or an error
    pub fn route_message(&self, message: &str) -> Result<String, FixRouterError> {
        self.router.route_message(message)
    }
    
    /// Shuts down all worker threads gracefully
    pub fn shutdown(self) {
        // Signal threads to shut down
        self.shutdown.store(true, Ordering::Relaxed);
        
        // Wait for all threads to finish
        for handle in self.worker_handles {
            let _ = handle.join();
        }
    }
}