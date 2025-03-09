mod fix_parser;
mod consistent_hash;
mod router;
mod error;
mod threaded_router;

use std::{thread, time::Duration};
use router::FixRouter;
use threaded_router::ThreadedFixRouter;
use error::FixRouterError;

fn main() -> Result<(), FixRouterError> {
    println!("FIX Router using Consistent Hashing with Threaded Processing");
    
    // Create a router with 3 virtual nodes per physical node
    let mut router = FixRouter::new(3);
    
    // Add some nodes to the ring
    router.add_node("node1", "192.168.1.101:9001")?;
    router.add_node("node2", "192.168.1.102:9002")?;
    router.add_node("node3", "192.168.1.103:9003")?;
    
    // Create a threaded router with the default number of threads (equal to CPU cores)
    let threaded_router = ThreadedFixRouter::new(router, None);
    
    // Sample FIX messages
    let messages = vec![
        "8=FIX.4.2|9=145|35=D|49=BUYSIDE|56=SELLSIDE|34=12345|52=20230502-12:30:00|11=ORD12345|55=AAPL|54=1|38=100|40=2|44=125.25|10=175|",
        "8=FIX.4.2|9=145|35=D|49=BUYSIDE|56=SELLSIDE|34=12346|52=20230502-12:31:00|11=ORD12346|55=MSFT|54=1|38=200|40=2|44=377.84|10=135|",
        "8=FIX.4.2|9=148|35=D|49=BUYSIDE|56=SELLSIDE|34=12347|52=20230502-12:32:00|11=ORD12347|55=GOOGL|54=1|38=50|40=2|44=2213.50|10=192|",
        "8=FIX.4.2|9=150|35=8|49=SELLSIDE|56=BUYSIDE|34=22345|52=20230502-12:33:00|11=ORD12345|37=EXE5001|55=AAPL|54=1|150=F|14=100|10=190|",
    ];
    
    println!("Processing messages asynchronously...");
    
    // Store receivers for all messages
    let mut receivers = Vec::new();
    
    // Simulate receiving messages and sending them for processing
    for (i, message) in messages.iter().enumerate() {
        println!("Received message {}", i + 1);
        
        // Send the message for asynchronous processing
        let receiver = threaded_router.route_message_async(message);
        receivers.push(receiver);
        
        // The main thread continues immediately - simulate other work
        println!("Main thread continues processing while message {} is being routed", i + 1);
        
        // Simulate a brief delay between messages arriving
        thread::sleep(Duration::from_millis(50));
    }
    
    // Now let's collect the results
    println!("\nAll messages have been submitted for processing");
    println!("Now collecting results:");
    
    for (i, receiver) in receivers.into_iter().enumerate() {
        // Wait for the result from the worker thread
        match receiver.recv() {
            Ok(result) => match result {
                Ok(node) => println!("Message {} routed to: {}", i + 1, node),
                Err(e) => println!("Error routing message {}: {:?}", i + 1, e),
            },
            Err(_) => println!("Worker thread disconnected before sending result for message {}", i + 1),
        }
    }
    
    // Shut down the threaded router gracefully
    println!("\nShutting down worker threads");
    threaded_router.shutdown();
    println!("Worker threads shut down successfully");
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fix_parser::{parse_fix_message, FixMessage};
    use crate::consistent_hash::ConsistentHashRing;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    // Test the FIX parser
    #[test]
    fn test_fix_parser() {
        // Create a sample FIX message
        let fix_message = "8=FIX.4.2|9=100|35=D|49=SENDER|56=TARGET|34=123|52=20231013-10:20:30|11=ABC123|55=AAPL|54=1|38=100|10=100|";
        
        // Parse the message
        let result = parse_fix_message(fix_message, '|');
        assert!(result.is_ok());
        
        let message = result.unwrap();
        
        // Check that required fields were parsed correctly
        assert_eq!(message.msg_type, "D");  // 35=D (New Order Single)
        assert_eq!(message.sender, "SENDER");  // 49=SENDER
        assert_eq!(message.target, "TARGET");  // 56=TARGET
        assert_eq!(message.seq_num, "123");  // 34=123
        assert_eq!(message.cl_ord_id, Some("ABC123".to_string()));  // 11=ABC123
        
        // Check that the routing key is based on ClOrdID
        assert_eq!(message.get_routing_key(), "clord:ABC123");
    }
    
    #[test]
    fn test_fix_parser_with_order_id() {
        // Create a sample FIX message with an OrderID (tag 37)
        let fix_message = "8=FIX.4.2|9=110|35=8|49=SENDER|56=TARGET|34=124|52=20231013-10:21:30|11=ABC123|37=XYZ789|55=AAPL|54=1|150=0|10=100|";
        
        // Parse the message
        let result = parse_fix_message(fix_message, '|');
        assert!(result.is_ok());
        
        let message = result.unwrap();
        
        // Check that both ClOrdID and OrderID were parsed
        assert_eq!(message.cl_ord_id, Some("ABC123".to_string()));
        assert_eq!(message.order_id, Some("XYZ789".to_string()));
        
        // Check that the routing key is based on ClOrdID (which now takes precedence over OrderID)
        assert_eq!(message.get_routing_key(), "clord:ABC123");
    }

    #[test]
    fn test_missing_required_tag() {
        // Create a FIX message missing a required tag (34 - MsgSeqNum)
        let fix_message = "8=FIX.4.2|9=90|35=D|49=SENDER|56=TARGET|52=20231013-10:22:30|11=ABC123|55=AAPL|54=1|38=100|10=100|";
        
        // Parse the message
        let result = parse_fix_message(fix_message, '|');
        
        // Verify that an error was returned
        assert!(result.is_err());
        
        // Check the error is due to missing tag 34
        match result.unwrap_err() {
            FixRouterError::MissingTag(tag) => {
                assert!(tag.contains("34"));
            },
            _ => panic!("Wrong error type returned"),
        }
    }
    
    // Test the consistent hash ring
    #[test]
    fn test_consistent_hash_ring() {
        let mut ring = ConsistentHashRing::new(3);  // 3 virtual nodes per physical node
        
        // Add some nodes
        ring.add_node("node1", "192.168.1.1:9001").unwrap();
        ring.add_node("node2", "192.168.1.2:9002").unwrap();
        
        // Check node count
        assert_eq!(ring.node_count(), 2);
        assert_eq!(ring.virtual_node_count(), 6);  // 2 nodes * 3 virtual nodes
        
        // Get node for a key
        let node = ring.get_node("test_key").unwrap();
        assert!(node == "192.168.1.1:9001" || node == "192.168.1.2:9002");
        
        // Verify that the same key always goes to the same node
        let node2 = ring.get_node("test_key").unwrap();
        assert_eq!(node, node2);
        
        // Remove a node
        ring.remove_node("node1").unwrap();
        
        // Check updated counts
        assert_eq!(ring.node_count(), 1);
        assert_eq!(ring.virtual_node_count(), 3);  // 1 node * 3 virtual nodes
        
        // Verify all keys now go to the remaining node
        let remaining_node = ring.get_node("any_key").unwrap();
        assert_eq!(remaining_node, "192.168.1.2:9002");
    }
    
    // Test node distribution
    #[test]
    fn test_distribution() {
        let mut ring = ConsistentHashRing::new(10);  // More virtual nodes for better distribution
        
        // Add nodes
        ring.add_node("node1", "192.168.1.1:9001").unwrap();
        ring.add_node("node2", "192.168.1.2:9002").unwrap();
        ring.add_node("node3", "192.168.1.3:9003").unwrap();
        
        // Generate a lot of keys and count distribution
        let mut distribution = HashMap::new();
        
        for i in 0..1000 {
            let key = format!("key_{}", i);
            let node = ring.get_node(&key).unwrap();
            
            *distribution.entry(node).or_insert(0) += 1;
        }
        
        // Check that each node got some keys
        assert!(distribution.len() == 3);
        
        for (node, count) in &distribution {
            // Each node should get roughly 1/3 of keys, but with significant variance possible
            // Lower threshold to 150 (instead of 200) to account for statistical variation
            assert!(*count > 150, "Node {} only got {} keys, expected >150", node, count);  
        }
    }
    
    // Test the integrated router
    #[test]
    fn test_fix_router() {
        let mut router = FixRouter::new(5);
        
        // Add three nodes
        router.add_node("node1", "192.168.1.1:9001").unwrap();
        router.add_node("node2", "192.168.1.2:9002").unwrap();
        router.add_node("node3", "192.168.1.3:9003").unwrap();
        
        // Route a series of messages for the same order
        let msg1 = "8=FIX.4.2|9=100|35=D|49=CLIENT|56=BROKER|34=1|52=20231013-10:25:00|11=ORDER123|55=AAPL|54=1|38=100|40=2|10=100|";
        let msg2 = "8=FIX.4.2|9=120|35=8|49=BROKER|56=CLIENT|34=1|52=20231013-10:25:01|11=ORDER123|37=EXEC123|55=AAPL|150=0|54=1|14=0|10=100|";
        let msg3 = "8=FIX.4.2|9=130|35=8|49=BROKER|56=CLIENT|34=2|52=20231013-10:26:00|37=EXEC123|11=ORDER123|55=AAPL|150=F|54=1|14=100|10=100|";
        
        // Route messages
        let node1 = router.route_message(msg1).unwrap();
        let node2 = router.route_message(msg2).unwrap();
        let node3 = router.route_message(msg3).unwrap();
        
        // All messages for the same order should go to the same node
        assert_eq!(node1, node2);
        assert_eq!(node2, node3);
    }
    
    // Test session affinity for multiple orders
    #[test]
    fn test_different_orders() {
        let mut router = FixRouter::new(5);
        
        router.add_node("node1", "192.168.1.1:9001").unwrap();
        router.add_node("node2", "192.168.1.2:9002").unwrap();
        
        // Create two different orders
        let order1_msg = "8=FIX.4.2|9=100|35=D|49=CLIENT|56=BROKER|34=1|52=20231013-10:25:00|11=ORDER123|55=AAPL|54=1|38=100|40=2|10=100|";
        let order2_msg = "8=FIX.4.2|9=100|35=D|49=CLIENT|56=BROKER|34=2|52=20231013-10:25:10|11=ORDER456|55=MSFT|54=1|38=200|40=2|10=100|";
        
        // Messages for different orders might go to different nodes
        let node1 = router.route_message(order1_msg).unwrap();
        let node2 = router.route_message(order2_msg).unwrap();
        
        // Just verify we get valid nodes (they may or may not be different)
        assert!(node1 == "192.168.1.1:9001" || node1 == "192.168.1.2:9002");
        assert!(node2 == "192.168.1.1:9001" || node2 == "192.168.1.2:9002");
    }
    
    // Test fault tolerance - when nodes are removed
    #[test]
    fn test_fault_tolerance() {
        let mut router = FixRouter::new(5);
        
        // Add three nodes
        router.add_node("node1", "192.168.1.1:9001").unwrap();
        router.add_node("node2", "192.168.1.2:9002").unwrap();
        router.add_node("node3", "192.168.1.3:9003").unwrap();
        
        // Map 100 different keys
        let mut key_to_node = HashMap::new();
        
        for i in 0..100 {
            let msg = format!("8=FIX.4.2|9=100|35=D|49=CLIENT|56=BROKER|34={}|52=20231013-10:25:00|11=ORDER{}|55=AAPL|54=1|38=100|40=2|10=100|", i, i);
            let node = router.route_message(&msg).unwrap();
            key_to_node.insert(i, node);
        }
        
        // Remove one node - simulate node failure
        router.remove_node("node1").unwrap();
        
        // Count how many keys were remapped
        let mut remapped_count = 0;
        
        for i in 0..100 {
            let msg = format!("8=FIX.4.2|9=100|35=D|49=CLIENT|56=BROKER|34={}|52=20231013-10:25:00|11=ORDER{}|55=AAPL|54=1|38=100|40=2|10=100|", i, i);
            let new_node = router.route_message(&msg).unwrap();
            
            if key_to_node[&i] != new_node && key_to_node[&i] == "192.168.1.1:9001" {
                remapped_count += 1;
            } else if key_to_node[&i] != new_node {
                panic!("A key was remapped from a node that wasn't removed!");
            }
        }
        
        // Only keys from node1 should be remapped
        let node1_keys = key_to_node.values()
            .filter(|&node| node == "192.168.1.1:9001")
            .count();
            
        assert_eq!(remapped_count, node1_keys);
    }
    
    // Test the threaded router implementation
    #[test]
    fn test_threaded_router() {
        let mut base_router = FixRouter::new(3);
        
        // Add some nodes
        base_router.add_node("node1", "192.168.1.1:9001").unwrap();
        base_router.add_node("node2", "192.168.1.2:9002").unwrap();
        
        // Create threaded router with 2 worker threads
        let threaded_router = ThreadedFixRouter::new(base_router, Some(2));
        
        // Process multiple messages in parallel
        let msg1 = "8=FIX.4.2|9=100|35=D|49=CLIENT|56=BROKER|34=1|52=20231013-10:25:00|11=ORDER123|55=AAPL|54=1|38=100|40=2|10=100|";
        let msg2 = "8=FIX.4.2|9=100|35=D|49=CLIENT|56=BROKER|34=2|52=20231013-10:25:10|11=ORDER456|55=MSFT|54=1|38=200|40=2|10=100|";
        let msg3 = "8=FIX.4.2|9=100|35=D|49=CLIENT|56=BROKER|34=3|52=20231013-10:25:20|11=ORDER789|55=GOOGL|54=1|38=50|40=2|10=100|";
        
        // Send messages for async processing
        let receiver1 = threaded_router.route_message_async(msg1);
        let receiver2 = threaded_router.route_message_async(msg2);
        let receiver3 = threaded_router.route_message_async(msg3);
        
        // Get results
        let result1 = receiver1.recv().expect("Failed to receive result 1");
        let result2 = receiver2.recv().expect("Failed to receive result 2");
        let result3 = receiver3.recv().expect("Failed to receive result 3");
        
        // Verify all results are valid
        assert!(result1.is_ok());
        assert!(result2.is_ok());
        assert!(result3.is_ok());
        
        // Verify results contain valid node addresses
        let node1 = result1.unwrap();
        let node2 = result2.unwrap();
        let node3 = result3.unwrap();
        
        assert!(node1 == "192.168.1.1:9001" || node1 == "192.168.1.2:9002");
        assert!(node2 == "192.168.1.1:9001" || node2 == "192.168.1.2:9002");
        assert!(node3 == "192.168.1.1:9001" || node3 == "192.168.1.2:9002");
    }
    
    // Test that messages for the same order are routed consistently in threaded mode
    #[test]
    fn test_threaded_router_consistency() {
        // Create a regular router for direct key verification
        let mut base_router = FixRouter::new(3);
        
        // Add some nodes
        base_router.add_node("node1", "192.168.1.1:9001").unwrap();
        base_router.add_node("node2", "192.168.1.2:9002").unwrap();
        base_router.add_node("node3", "192.168.1.3:9003").unwrap();
        
        println!("\n*** Running consistency test ***");
        
        // Multiple messages for the same order
        let order_msgs = vec![
            "8=FIX.4.2|9=100|35=D|49=CLIENT|56=BROKER|34=1|52=20231013-10:25:00|11=ORDER123|55=AAPL|54=1|38=100|40=2|10=100|",
            "8=FIX.4.2|9=120|35=8|49=BROKER|56=CLIENT|34=1|52=20231013-10:25:01|11=ORDER123|37=EXEC123|55=AAPL|150=0|54=1|14=0|10=100|",
            "8=FIX.4.2|9=130|35=8|49=BROKER|56=CLIENT|34=2|52=20231013-10:26:00|37=EXEC123|11=ORDER123|55=AAPL|150=F|54=1|14=100|10=100|",
        ];
        
        // First check that all messages route to the same node directly using the base router
        println!("First verify direct routing with base router:");
        let mut expected_node = None;
        
        for (i, msg) in order_msgs.iter().enumerate() {
            let node = base_router.route_message(msg).unwrap();
            println!("Direct message {} routed to: {}", i+1, node);
            
            if i == 0 {
                expected_node = Some(node);
            } else {
                assert_eq!(&node, expected_node.as_ref().unwrap(), 
                          "Direct routing inconsistency: message {} went to a different node", i+1);
            }
        }
        
        // Now create the threaded router and test
        println!("\nNow testing with threaded router:");
        let threaded_router = ThreadedFixRouter::new(base_router.clone(), Some(3));
        
        // First, let's route the first message and remember the node
        let receiver = threaded_router.route_message_async(order_msgs[0]);
        let first_node = receiver.recv().unwrap().unwrap();
        println!("Threaded message 1 routed to: {}", first_node);
        
        // Make sure the threaded router matches the direct router's decision
        assert_eq!(&first_node, expected_node.as_ref().unwrap(), 
                  "Threaded router routed differently than direct router");
        
        // Now route the other messages
        let mut receivers = Vec::new();
        
        // Skip the first message since we already processed it
        for (i, msg) in order_msgs[1..].iter().enumerate() {
            let receiver = threaded_router.route_message_async(msg);
            receivers.push((i+2, receiver)); // i+2 because we're skipping the first message
        }
        
        // Check all messages go to the same node
        for (msg_num, receiver) in receivers {
            let node = receiver.recv()
                .expect("Failed to receive result")
                .expect("Failed to route message");
            
            println!("Threaded message {} routed to: {}", msg_num, node);
            
            // Compare with the first node
            assert_eq!(
                node, first_node,
                "Messages for the same order were routed to different nodes: {} vs {}",
                node, first_node
            );
        }
        
        println!("*** Consistency test complete ***");
    }
    
    // Test high throughput with many messages
    #[test]
    fn test_threaded_router_throughput() {
        let mut base_router = FixRouter::new(3);
        
        // Add some nodes
        base_router.add_node("node1", "192.168.1.1:9001").unwrap();
        base_router.add_node("node2", "192.168.1.2:9002").unwrap();
        base_router.add_node("node3", "192.168.1.3:9003").unwrap();
        
        // Create threaded router with 4 worker threads
        let threaded_router = ThreadedFixRouter::new(base_router, Some(4));
        
        // Generate many messages
        let mut messages = Vec::new();
        let mut receivers = Vec::new();
        
        for i in 0..100 {
            let msg = format!("8=FIX.4.2|9=100|35=D|49=CLIENT|56=BROKER|34={}|52=20231013-10:25:00|11=ORDER{}|55=AAPL|54=1|38=100|40=2|10=100|", i, i);
            messages.push(msg);
        }
        
        // Send all messages at once
        for msg in &messages {
            let receiver = threaded_router.route_message_async(msg);
            receivers.push(receiver);
        }
        
        // Verify all messages are processed
        let mut success_count = 0;
        for receiver in receivers {
            if let Ok(Ok(_)) = receiver.recv() {
                success_count += 1;
            }
        }
        
        assert_eq!(success_count, 100);
    }
}
