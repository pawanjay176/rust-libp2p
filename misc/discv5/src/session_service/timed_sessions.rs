//! A simple data structure for managing the timeouts of sessions.
//!
//! This stores a hashmap of Sessions coupled with a delay queue to indicate when a session has
//! expired.

use crate::session::Session;
use core::pin::Pin;
use enr::NodeId;
use futures::{Stream, StreamExt};
use std::collections::HashMap;
use std::task::{self, Poll};
use std::time::Duration;
use tokio::time::{delay_queue, DelayQueue};

/// A collection of sessions and associated timeouts.
///
/// Sessions have an establishment timeout as
/// well as lifetime.
pub struct TimedSessions {
    /// The sessions being established.
    sessions: HashMap<NodeId, (Session, delay_queue::Key)>,
    /// A queue indicating when a session has timed out.
    timeouts: DelayQueue<NodeId>,
    /// The time to wait for a session to be established.
    session_establish_timeout: Duration,
}

impl TimedSessions {
    pub fn new(session_establish_timeout: Duration) -> Self {
        TimedSessions {
            sessions: HashMap::new(),
            timeouts: DelayQueue::new(),
            session_establish_timeout,
        }
    }

    pub fn insert(&mut self, node_id: NodeId, session: Session) {
        self.insert_at(node_id, session, self.session_establish_timeout);
    }

    pub fn insert_at(&mut self, node_id: NodeId, session: Session, duration: Duration) {
        let delay = self.timeouts.insert(node_id.clone(), duration);

        self.sessions.insert(node_id, (session, delay));
    }

    pub fn get(&self, node_id: &NodeId) -> Option<&Session> {
        self.sessions.get(node_id).map(|&(ref v, _)| v)
    }

    pub fn get_mut(&mut self, node_id: &NodeId) -> Option<&mut Session> {
        self.sessions.get_mut(node_id).map(|(v, _)| v)
    }

    pub fn update_timeout(&mut self, node_id: &NodeId, timeout: Duration) {
        if let Some((_, key)) = self.sessions.get(node_id) {
            self.timeouts.reset(key, timeout);
        }
    }

    pub fn remove(&mut self, node_id: &NodeId) {
        if let Some((_, delay_key)) = self.sessions.remove(node_id) {
            self.timeouts.remove(&delay_key);
        }
    }
}

impl Stream for TimedSessions {
    type Item = Result<(NodeId, Session), &'static str>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        let timed_session = self.get_mut();
        match timed_session.timeouts.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(node_id))) => {
                let node_id = node_id.into_inner();
                match timed_session.sessions.remove(&node_id) {
                    Some((session, _)) => Poll::Ready(Some(Ok((node_id, session)))),
                    None => Poll::Ready(Some(Err("Session no longer exists"))),
                }
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Err(_))) => Poll::Ready(Some(Err("Session delay queue error"))),
        }
    }
}
