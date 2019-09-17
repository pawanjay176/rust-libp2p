use sha3::{Digest, Keccak256};
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Max allowed node entries across all topics.
const MAX_ENTRIES: usize = 1000;
/// Max allowed entries within a topic queue.
const MAX_ENTRIES_PER_TOPIC: usize = 50;

pub type TopicHash = [u8; 32];

/// TODO: change to some unique identifier type
pub type TicketId = String;

/// Representation of a ticket issued to peer for topic registration.
#[derive(Debug, Clone)]
pub struct Ticket<TPeerId> {
    /// Unique identifier for ticket.
    id: TicketId,
    /// Id of peer to which ticket is issued.
    peer_id: TPeerId,
    /// Wait time for ticket to be allowed for topic registration.
    wait_time: Duration,
    /// Time instant at which ticket was registered
    created_time: Instant,
}

impl<TPeerId> Ticket<TPeerId> {
    pub fn new(peer: TPeerId, wait_time: u64) -> Self {
        Ticket {
            id: String::from("test"), // TODO
            peer_id: peer,
            wait_time: Duration::from_secs(wait_time),
            created_time: Instant::now(),
        }
    }

    /// Checks if wait time for ticket has passed.
    pub fn has_wait_elapsed(&self) -> bool {
        if self.created_time + self.wait_time < Instant::now() {
            return false;
        } else {
            return true;
        }
    }
}

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Clone)]
pub struct Topic(String);

impl Topic {
    pub fn get_topic_hash(&self) -> TopicHash {
        let mut hasher = Keccak256::new();
        hasher.input(&self.0);
        let mut result: [u8; 32] = std::default::Default::default();
        result.clone_from_slice(hasher.result().as_slice());
        result
    }
}

#[derive(Debug)]
pub struct TopicQueue<TPeerId> {
    topic: Topic,
    queue: VecDeque<(TPeerId, Instant)>,
}

impl<TPeerId> TopicQueue<TPeerId> {
    pub fn new(topic: Topic) -> Self {
        TopicQueue {
            topic,
            queue: VecDeque::with_capacity(MAX_ENTRIES_PER_TOPIC),
        }
    }

    pub fn size(&self) -> usize {
        self.queue.len()
    }

    /// Add a peer to the topic queue.
    pub fn add_to_queue(&mut self, peer: TPeerId) {
        if self.queue.len() == MAX_ENTRIES_PER_TOPIC {
            self.remove_from_queue();
        }
        self.queue.push_back((peer, Instant::now()));
    }

    /// Remove element from queue according to some policy
    pub fn remove_from_queue(&mut self) {
        unimplemented!()
    }

    /// Get wait time for queue.
    pub fn get_wait_time(&self) -> u64 {
        unimplemented!()
    }
}

/// Global queue containing all topic queues and issued tickets
/// TODO: Change name to something less atrocious
#[derive(Debug)]
pub struct GlobalTopicQueue<TPeerId> {
    topic_map: BTreeMap<Topic, TopicQueue<TPeerId>>,
    tickets: BTreeMap<TicketId, Ticket<TPeerId>>,
}

impl<TPeerId> GlobalTopicQueue<TPeerId>
where
    TPeerId: Clone,
{
    pub fn new() -> Self {
        GlobalTopicQueue {
            topic_map: BTreeMap::new(),
            tickets: BTreeMap::new(),
        }
    }

    /// Get combined size of all peers across all topic queues.
    fn get_queue_size(&self) -> usize {
        self.topic_map.iter().map(|(_, v)| v.size()).sum()
    }

    /// Add a peer to the topic queue.
    /// Returns None if ticket doesn't exist or wait time hasn't elapsed.
    pub fn add_to_queue(&mut self, peer: TPeerId, topic: Topic, ticket: &TicketId) -> Option<()> {
        if !self.is_ticket_valid(ticket) {
            return None;
        }
        if self.get_queue_size() == MAX_ENTRIES {
            self.remove_from_queue();
        }
        if let Some(queue) = self.topic_map.get_mut(&topic) {
            queue.add_to_queue(peer);
        } else {
            let mut tq = TopicQueue::new(topic.clone());
            tq.add_to_queue(peer);
            self.topic_map.insert(topic, tq);
        };
        Some(())
    }

    /// Remove element from one of the queues according to some policy
    pub fn remove_from_queue(&mut self) {
        unimplemented!()
    }

    pub fn issue_ticket(&mut self, peer: TPeerId, topic: Topic) -> TicketId {
        let wait_time = self
            .topic_map
            .get(&topic)
            .map(|v| v.get_wait_time())
            .unwrap_or(0);
        let ticket = Ticket::new(peer, wait_time);
        self.tickets.insert(ticket.id.clone(), ticket.clone());
        ticket.id
    }

    /// Checks if ticket is registered in map and the wait time has elapsed.
    pub fn is_ticket_valid(&self, ticket_id: &TicketId) -> bool {
        if let Some(ticket) = self.tickets.get(ticket_id) {
            ticket.has_wait_elapsed();
        }
        false
    }
}