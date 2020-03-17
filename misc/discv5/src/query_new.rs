// Copyright 2019 Parity Technologies (UK) Ltd.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

mod peers;

use peers::closest::{FindNodeQuery, FindNodeQueryConfig};
use peers::{QueryState, ReturnPeer};

use crate::kbucket::Key;
use fnv::FnvHashMap;

/// A `QueryPool` provides an aggregate state machine for driving `Query`s to completion.
///
/// Internally, a `Query` is in turn driven by an underlying `QueryPeerIter`
/// that determines the peer selection strategy, i.e. the order in which the
/// peers involved in the query should be contacted.
pub struct QueryPool<TInner, TTarget, TNodeId> {
    next_id: usize,
    queries: FnvHashMap<QueryId, Query<TInner, TTarget, TNodeId>>,
}

/// The observable states emitted by [`QueryPool::poll`].
pub enum QueryPoolState<'a, TInner, TTarget, TNodeId> {
    /// The pool is idle, i.e. there are no queries to process.
    Idle,
    /// At least one query is waiting for results. `Some(request)` indicates
    /// that a new request is now being waited on.
    Waiting(Option<(&'a mut Query<TInner, TTarget, TNodeId>, ReturnPeer<TNodeId>)>),
    /// A query has finished.
    Finished(Query<TInner, TTarget, TNodeId>),
}

impl<TInner, TTarget, TNodeId> QueryPool<TInner, TTarget, TNodeId>
where
    TTarget: Into<Key<TTarget>> + Clone,
    TNodeId: Into<Key<TNodeId>> + Eq + Clone,
{
    /// Creates a new `QueryPool` with the given configuration.
    pub fn new() -> Self {
        QueryPool {
            next_id: 0,
            queries: Default::default(),
        }
    }

    /// Returns an iterator over the queries in the pool.
    pub fn iter(&self) -> impl Iterator<Item = &Query<TInner, TTarget, TNodeId>> {
        self.queries.values()
    }

    /// Gets the current size of the pool, i.e. the number of running queries.
    pub fn size(&self) -> usize {
        self.queries.len()
    }

    /// Returns an iterator that allows modifying each query in the pool.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut Query<TInner, TTarget, TNodeId>> {
        self.queries.values_mut()
    }

    /// Adds a query to the pool that iterates towards the closest peers to the target.
    pub fn add_findnode_query<T, I>(&mut self, target: TTarget, peers: I, inner: TInner) -> QueryId
    where
        I: IntoIterator<Item = Key<TNodeId>> + Eq + Clone,
    {
        let cfg = FindNodeQueryConfig::default();
        // Should be a passed parameter (Mostly Option<usize>)
        let iterations = 3;
        let findnode_query = FindNodeQuery::with_config(cfg, target, peers, iterations);
        let peer_iter = QueryPeerIter::FindNode(findnode_query);
        self.add(peer_iter, inner)
    }

    fn add(&mut self, peer_iter: QueryPeerIter<TTarget, TNodeId>, inner: TInner) -> QueryId {
        let id = QueryId(self.next_id);
        self.next_id = self.next_id.wrapping_add(1);
        let query = Query::new(id, peer_iter, inner);
        self.queries.insert(id, query);
        id
    }

    /// Returns a reference to a query with the given ID, if it is in the pool.
    pub fn get(&self, id: &QueryId) -> Option<&Query<TInner, TTarget, TNodeId>> {
        self.queries.get(id)
    }

    /// Returns a mutablereference to a query with the given ID, if it is in the pool.
    pub fn get_mut(&mut self, id: &QueryId) -> Option<&mut Query<TInner, TTarget, TNodeId>> {
        self.queries.get_mut(id)
    }

    /// Polls the pool to advance the queries.
    pub fn poll(&mut self) -> QueryPoolState<TInner, TTarget, TNodeId> {
        let mut finished = None;
        let mut waiting = None;

        for (&query_id, query) in self.queries.iter_mut() {
            match query.next() {
                QueryState::Finished => {
                    finished = Some(query_id);
                    break;
                }
                QueryState::Waiting(Some(return_peer)) => {
                    waiting = Some((query_id, return_peer));
                    break;
                }
                QueryState::Waiting(None) | QueryState::WaitingAtCapacity => {}
            }
        }

        if let Some((query_id, return_peer)) = waiting {
            let query = self.queries.get_mut(&query_id).expect("s.a.");
            return QueryPoolState::Waiting(Some((query, return_peer)));
        }

        if let Some(query_id) = finished {
            let query = self.queries.remove(&query_id).expect("s.a.");
            return QueryPoolState::Finished(query);
        }

        if self.queries.is_empty() {
            return QueryPoolState::Idle;
        } else {
            return QueryPoolState::Waiting(None);
        }
    }
}

/// Unique identifier for an active query.
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub struct QueryId(usize);

/// A query in a `QueryPool`.
pub struct Query<TInner, TTarget, TNodeId> {
    /// The unique ID of the query.
    id: QueryId,
    /// The peer iterator that drives the query state.
    peer_iter: QueryPeerIter<TTarget, TNodeId>,
    /// The opaque inner query state.
    pub inner: TInner,
}

/// The peer selection strategies that can be used by queries.
enum QueryPeerIter<TTarget, TNodeId> {
    FindNode(FindNodeQuery<TTarget, TNodeId>),
}

impl<TInner, TTarget, TNodeId> Query<TInner, TTarget, TNodeId>
where
    TTarget: Into<Key<TTarget>> + Clone,
    TNodeId: Into<Key<TNodeId>> + Eq + Clone,
{
    /// Creates a new query without starting it.
    fn new(id: QueryId, peer_iter: QueryPeerIter<TTarget, TNodeId>, inner: TInner) -> Self {
        Query {
            id,
            inner,
            peer_iter,
        }
    }

    /// Gets the unique ID of the query.
    pub fn id(&self) -> QueryId {
        self.id
    }

    /// Informs the query that the attempt to contact `peer` failed.
    pub fn on_failure(&mut self, peer: &TNodeId) {
        match &mut self.peer_iter {
            QueryPeerIter::FindNode(iter) => iter.on_failure(peer),
        }
    }

    /// Informs the query that the attempt to contact `peer` succeeded,
    /// possibly resulting in new peers that should be incorporated into
    /// the query, if applicable.
    pub fn on_success<I>(&mut self, peer: &TNodeId, new_peers: Vec<TNodeId>) {
        match &mut self.peer_iter {
            QueryPeerIter::FindNode(iter) => iter.on_success(peer, new_peers),
        }
    }

    /// Advances the state of the underlying peer iterator.
    fn next(&mut self) -> QueryState<TNodeId> {
        match &mut self.peer_iter {
            QueryPeerIter::FindNode(iter) => iter.next(),
        }
    }

    /// Consumes the query, producing the final `QueryResult`.
    pub fn into_result(self) -> QueryResult<TInner, impl Iterator<Item = TNodeId>> {
        let peers = match self.peer_iter {
            QueryPeerIter::FindNode(iter) => iter.into_result(),
        };
        QueryResult {
            inner: self.inner,
            peers,
        }
    }
}

/// The result of a `Query`.
pub struct QueryResult<TInner, TNodeId> {
    /// The opaque inner query state.
    pub inner: TInner,
    /// The successfully contacted peers.
    pub peers: TNodeId,
}
