use super::*;
use crate::kbucket::{Distance, Key, MAX_NODES_PER_BUCKET};
use std::collections::btree_map::{BTreeMap, Entry};
use std::iter::FromIterator;

pub struct PredicateQuery<TTarget, TNodeId> {
    /// Target we're looking for.
    target: TTarget,

    /// The target key we are looking for
    target_key: Key<TTarget>,

    /// The current state of progress of the query.
    progress: QueryProgress,

    /// The closest peers to the target, ordered by increasing distance.
    closest_peers: BTreeMap<Distance, QueryPeer<TNodeId>>,

    /// Maximum RPC iterations per peer.
    iterations: usize,

    /// The number of peers for which the query is currently waiting for results.
    num_waiting: usize,

    /// The predicate function to be applied to filter the enr's found during the search.
    predicate: Box<dyn Fn(&TNodeId, &[u8]) -> bool + 'static>,

    /// The value to be passed to the predicate function to match against the enr value.
    value: Vec<u8>,

    /// Peers satisfying the predicate.
    peers: Vec<TNodeId>,

    /// The configuration of the query.
    config: PredicateQueryConfig,
}

/// Configuration for a `Query`.
#[derive(Debug, Clone)]
pub struct PredicateQueryConfig {
    /// Allowed level of parallelism.
    ///
    /// The `α` parameter in the Kademlia paper. The maximum number of peers that a query
    /// is allowed to wait for in parallel while iterating towards the closest
    /// nodes to a target. Defaults to `3`.
    pub parallelism: usize,

    /// Number of results to produce.
    ///
    /// The number of closest peers that a query must obtain successful results
    /// for before it terminates. Defaults to the maximum number of entries in a
    /// single k-bucket, i.e. the `k` parameter in the Kademlia paper.
    pub num_results: usize,
}

impl Default for PredicateQueryConfig {
    fn default() -> Self {
        PredicateQueryConfig {
            parallelism: 3,
            num_results: MAX_NODES_PER_BUCKET,
        }
    }
}

impl<TTarget, TNodeId> PredicateQuery<TTarget, TNodeId>
where
    TTarget: Into<Key<TTarget>> + Clone,
    TNodeId: Into<Key<TNodeId>> + Clone + Eq,
{
    /// Creates a new query with the given configuration.
    pub fn with_config<I>(
        config: PredicateQueryConfig,
        target: TTarget,
        known_closest_peers: I,
        iterations: usize,
        predicate: impl Fn(&TNodeId, &[u8]) -> bool + 'static,
        value: Vec<u8>,
    ) -> Self
    where
        I: IntoIterator<Item = Key<TNodeId>>,
    {
        let target_key = target.clone().into();

        // Initialise the closest peers to begin the query with.
        let closest_peers = BTreeMap::from_iter(
            known_closest_peers
                .into_iter()
                .map(|key| {
                    let distance = key.distance(&target_key);
                    let state = QueryPeerState::NotContacted;
                    (distance, QueryPeer::new(key, state))
                })
                .take(config.num_results),
        );

        // The query initially makes progress by iterating towards the target.
        let progress = QueryProgress::Iterating { no_progress: 0 };

        PredicateQuery {
            config,
            target,
            target_key,
            progress,
            closest_peers,
            iterations,
            num_waiting: 0,
            predicate: Box::new(predicate),
            value,
            peers: Vec::new(),
        }
    }

    /// Borrows the underlying target of the query.
    pub fn target(&self) -> &TTarget {
        &self.target
    }

    /// Mutably borrows the underlying target of the query.
    pub fn target_mut(&mut self) -> &mut TTarget {
        &mut self.target
    }

    /// Callback for delivering the result of a successful request to a peer
    /// that the query is waiting on.
    ///
    /// Delivering results of requests back to the query allows the query to make
    /// progress. The query is said to make progress either when the given
    /// `closer_peers` contain a peer closer to the target than any peer seen so far,
    /// or when the query did not yet accumulate `num_results` closest peers and
    /// `closer_peers` contains a new peer, regardless of its distance to the target.
    ///
    /// After calling this function, `next` should eventually be called again
    /// to advance the state of the query.
    ///
    /// If the query is finished, the query is not currently waiting for a
    /// result from `peer`, or a result for `peer` has already been reported,
    /// calling this function has no effect.
    pub fn on_success(&mut self, node_id: &TNodeId, closer_peers: Vec<TNodeId>) {
        if let QueryProgress::Finished = self.progress {
            return;
        }

        let key = node_id.clone().into();
        let distance = key.distance(&self.target_key);
        let num_closest = self.closest_peers.len();

        // Mark the peer's progress, the total nodes it has returned and it's current iteration.
        // If iterations have been completed and the node returned peers, mark it as succeeded.
        match self.closest_peers.entry(distance) {
            Entry::Vacant(..) => return,
            Entry::Occupied(mut e) => match e.get().state {
                QueryPeerState::Waiting => {
                    debug_assert!(self.num_waiting > 0);
                    self.num_waiting -= 1;
                    let peer = e.get_mut();
                    peer.peers_returned += num_closest;
                    if peer.peers_returned >= self.config.num_results {
                        peer.state = QueryPeerState::Succeeded;
                    } else if self.iterations == peer.iteration {
                        if peer.peers_returned > 0 {
                            // mark the peer as succeeded
                            peer.state = QueryPeerState::Succeeded;
                        } else {
                            peer.state = QueryPeerState::Failed; // didn't return any peers
                        }
                    } else {
                        // still have iteration's to complete
                        peer.iteration += 1;
                        peer.state = QueryPeerState::PendingIteration;
                    }
                }
                QueryPeerState::NotContacted
                | QueryPeerState::Failed
                | QueryPeerState::PendingIteration
                | QueryPeerState::Succeeded => return,
            },
        }

        let mut progress = false;

        // Incorporate the reported closer peers into the query.
        for enr in closer_peers {
            let key = enr.into();
            let distance = self.target_key.distance(&key);
            let peer = QueryPeer::new(key, QueryPeerState::NotContacted);
            self.closest_peers.entry(distance).or_insert(peer);
            // If enr satisfies the predicate, add to list of peers that satisfies predicate
            if (self.predicate)(&enr, &self.value) {
                self.peers.push(enr);
            }
            // The query makes progress if the new peer is either closer to the target
            // than any peer seen so far (i.e. is the first entry), or the query did
            // not yet accumulate enough closest peers.
            progress = self.closest_peers.keys().next() == Some(&distance)
                || num_closest < self.config.num_results;
        }

        // Update the query progress.
        self.progress = match self.progress {
            QueryProgress::Iterating { no_progress } => {
                let no_progress = if progress { 0 } else { no_progress + 1 };
                if no_progress >= self.config.parallelism * self.iterations {
                    QueryProgress::Stalled
                } else {
                    QueryProgress::Iterating { no_progress }
                }
            }
            QueryProgress::Stalled => {
                if progress {
                    QueryProgress::Iterating { no_progress: 0 }
                } else {
                    QueryProgress::Stalled
                }
            }
            QueryProgress::Finished => QueryProgress::Finished,
        }
    }

    /// Callback for informing the query about a failed request to a peer
    /// that the query is waiting on.
    ///
    /// After calling this function, `next` should eventually be called again
    /// to advance the state of the query.
    ///
    /// If the query is finished, the query is not currently waiting for a
    /// result from `peer`, or a result for `peer` has already been reported,
    /// calling this function has no effect.
    pub fn on_failure(&mut self, peer: &TNodeId) {
        if let QueryProgress::Finished = self.progress {
            return;
        }

        let key = peer.clone().into();
        let distance = key.distance(&self.target_key);

        match self.closest_peers.entry(distance) {
            Entry::Vacant(_) => {}
            Entry::Occupied(mut e) => match e.get().state {
                QueryPeerState::Waiting => {
                    debug_assert!(self.num_waiting > 0);
                    self.num_waiting -= 1;
                    e.get_mut().state = QueryPeerState::Failed
                }
                _ => {}
            },
        }
    }

    /// Advances the state of the query, potentially getting a new peer to contact.
    ///
    /// See [`QueryState`].
    pub fn next(&mut self) -> QueryState<TNodeId> {
        if let QueryProgress::Finished = self.progress {
            return QueryState::Finished;
        }

        // Count the number of peers that returned a result. If there is a
        // request in progress to one of the `num_results` closest peers, the
        // counter is set to `None` as the query can only finish once
        // `num_results` closest peers have responded (or there are no more
        // peers to contact, see `active_counter`).
        let mut result_counter = Some(0);

        // Check if the query is at capacity w.r.t. the allowed parallelism.
        let at_capacity = self.at_capacity();

        for peer in self.closest_peers.values_mut() {
            match peer.state {
                QueryPeerState::PendingIteration | QueryPeerState::NotContacted => {
                    // This peer is waiting to be reiterated.
                    if !at_capacity {
                        peer.state = QueryPeerState::Waiting;
                        self.num_waiting += 1;
                        let return_peer = ReturnPeer {
                            node_id: peer.key.preimage().clone(),
                            iteration: peer.iteration,
                        };
                        return QueryState::Waiting(Some(return_peer));
                    } else {
                        return QueryState::WaitingAtCapacity;
                    }
                }

                QueryPeerState::Waiting => {
                    if at_capacity {
                        // The query is still waiting for a result from a peer and is
                        // at capacity w.r.t. the maximum number of peers being waited on.
                        return QueryState::WaitingAtCapacity;
                    } else {
                        // The query is still waiting for a result from a peer and the
                        // `result_counter` did not yet reach `num_results`. Therefore
                        // the query is not yet done, regardless of already successful
                        // queries to peers farther from the target.
                        result_counter = None;
                    }
                }

                QueryPeerState::Succeeded => {
                    if let Some(ref mut cnt) = result_counter {
                        *cnt += 1;
                        // If `num_results` successful results have been delivered for the
                        // closest peers, the query is done.
                        if *cnt >= self.config.num_results {
                            self.progress = QueryProgress::Finished;
                            return QueryState::Finished;
                        }
                    }
                }

                QueryPeerState::Failed => {
                    // Skip over unresponsive or failed peers.
                }
            }
        }

        if self.num_waiting > 0 {
            // The query is still waiting for results and not at capacity w.r.t.
            // the allowed parallelism, but there are no new peers to contact
            // at the moment.
            QueryState::Waiting(None)
        } else {
            // The query is finished because all available peers have been contacted
            // and the query is not waiting for any more results.
            self.progress = QueryProgress::Finished;
            QueryState::Finished
        }
    }

    /// Consumes the query, returning the peers who match the predicate.
    pub fn into_result(self) -> impl Iterator<Item = TNodeId> {
        self.peers.into_iter().take(self.config.num_results)
    }

    /// Checks if the query is at capacity w.r.t. the permitted parallelism.
    ///
    /// While the query is stalled, up to `num_results` parallel requests
    /// are allowed. This is a slightly more permissive variant of the
    /// requirement that the initiator "resends the FIND_NODE to all of the
    /// k closest nodes it has not already queried".
    fn at_capacity(&self) -> bool {
        match self.progress {
            QueryProgress::Stalled => self.num_waiting >= self.config.num_results,
            QueryProgress::Iterating { .. } => self.num_waiting >= self.config.parallelism,
            QueryProgress::Finished => true,
        }
    }
}

/// Stage of the query.
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
enum QueryProgress {
    /// The query is making progress by iterating towards `num_results` closest
    /// peers to the target with a maximum of `parallelism` peers for which the
    /// query is waiting for results at a time.
    ///
    /// > **Note**: When the query switches back to `Iterating` after being
    /// > `Stalled`, it may temporarily be waiting for more than `parallelism`
    /// > results from peers, with new peers only being considered once
    /// > the number pending results drops below `parallelism`.
    Iterating {
        /// The number of consecutive results that did not yield a peer closer
        /// to the target. When this number reaches `parallelism` and no new
        /// peer was discovered or at least `num_results` peers are known to
        /// the query, it is considered `Stalled`.
        no_progress: usize,
    },

    /// A query is stalled when it did not make progress after `parallelism`
    /// consecutive successful results (see `on_success`).
    ///
    /// While the query is stalled, the maximum allowed parallelism for pending
    /// results is increased to `num_results` in an attempt to finish the query.
    /// If the query can make progress again upon receiving the remaining
    /// results, it switches back to `Iterating`. Otherwise it will be finished.
    Stalled,

    /// The query is finished.
    ///
    /// A query finishes either when it has collected `num_results` results
    /// from the closest peers (not counting those that failed or are unresponsive)
    /// or because the query ran out of peers that have not yet delivered
    /// results (or failed).
    Finished,
}

/// Representation of a peer in the context of a query.
#[derive(Debug, Clone)]
struct QueryPeer<TNodeId> {
    /// The `KBucket` key used to identify the peer.
    key: Key<TNodeId>,

    /// The current rpc request iteration that has been made on this peer.
    iteration: usize,

    /// The number of peers that have been returned by this peer.
    peers_returned: usize,

    /// The current query state of this peer.
    state: QueryPeerState,
}

impl<TNodeId> QueryPeer<TNodeId> {
    pub fn new(key: Key<TNodeId>, state: QueryPeerState) -> Self {
        QueryPeer {
            key,
            iteration: 1,
            peers_returned: 0,
            state,
        }
    }
}

/// The state of `QueryPeer` in the context of a query.
#[derive(Debug, Copy, Clone)]
enum QueryPeerState {
    /// The peer has not yet been contacted.
    ///
    /// This is the starting state for every peer known to, or discovered by, a query.
    NotContacted,

    /// The query is waiting for a result from the peer.
    Waiting,

    /// The peer is waiting to to begin another iteration.
    PendingIteration,

    /// Obtaining a result from the peer has failed.
    ///
    /// This is a final state, reached as a result of a call to `on_failure`.
    Failed,

    /// A successful result from the peer has been delivered.
    ///
    /// This is a final state, reached as a result of a call to `on_success`.
    Succeeded,
}