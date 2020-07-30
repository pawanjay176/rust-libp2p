// Copyright 2020 Sigma Prime Pty Ltd.
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

// collection of tests for the gossipsub network behaviour

#[cfg(test)]
mod tests {
    use std::thread::sleep;
    use std::time::Duration;

    use crate::{GossipsubConfigBuilder, IdentTopic as Topic};

    use super::super::*;

    // helper functions for testing

    fn build_and_inject_nodes(
        peer_no: usize,
        topics: Vec<String>,
        to_subscribe: bool,
    ) -> (Gossipsub, Vec<PeerId>, Vec<TopicHash>) {
        // use a default GossipsubConfig
        build_and_inject_nodes_with_config(
            peer_no,
            topics,
            to_subscribe,
            GossipsubConfig::default(),
        )
    }

    fn build_and_inject_nodes_with_config(
        peer_no: usize,
        topics: Vec<String>,
        to_subscribe: bool,
        gs_config: GossipsubConfig,
    ) -> (Gossipsub, Vec<PeerId>, Vec<TopicHash>) {
        // create a gossipsub struct
        build_and_inject_nodes_with_config_and_explicit(peer_no, topics, to_subscribe, gs_config, 0)
    }

    // This function generates `peer_no` random PeerId's, subscribes to `topics` and subscribes the
    // injected nodes to all topics if `to_subscribe` is set. All nodes are considered gossipsub nodes.
    fn build_and_inject_nodes_with_config_and_explicit(
        peer_no: usize,
        topics: Vec<String>,
        to_subscribe: bool,
        gs_config: GossipsubConfig,
        explicit: usize,
    ) -> (Gossipsub, Vec<PeerId>, Vec<TopicHash>) {
        let keypair = libp2p_core::identity::Keypair::generate_secp256k1();
        // create a gossipsub struct
        let mut gs: Gossipsub = Gossipsub::new(MessageAuthenticity::Signed(keypair), gs_config);

        let mut topic_hashes = vec![];

        // subscribe to the topics
        for t in topics {
            let topic = Topic::new(t);
            gs.subscribe(topic.clone());
            topic_hashes.push(topic.hash().clone());
        }

        // build and connect peer_no random peers
        let mut peers = vec![];

        for i in 0..peer_no {
            let peer = PeerId::random();
            peers.push(peer.clone());
            <Gossipsub as NetworkBehaviour>::inject_connected(&mut gs, &peer);
            if i < explicit {
                gs.add_explicit_peer(&peer);
            }
            if to_subscribe {
                gs.handle_received_subscriptions(
                    &topic_hashes
                        .iter()
                        .cloned()
                        .map(|t| GossipsubSubscription {
                            action: GossipsubSubscriptionAction::Subscribe,
                            topic_hash: t,
                        })
                        .collect::<Vec<_>>(),
                    &peer,
                );
            };
        }

        return (gs, peers, topic_hashes);
    }

    #[test]
    /// Test local node subscribing to a topic
    fn test_subscribe() {
        // The node should:
        // - Create an empty vector in mesh[topic]
        // - Send subscription request to all peers
        // - run JOIN(topic)

        let subscribe_topic = vec![String::from("test_subscribe")];
        let (gs, _, topic_hashes) = build_and_inject_nodes(20, subscribe_topic, true);

        assert!(
            gs.mesh.get(&topic_hashes[0]).is_some(),
            "Subscribe should add a new entry to the mesh[topic] hashmap"
        );

        // collect all the subscriptions
        let subscriptions =
            gs.events
                .iter()
                .fold(vec![], |mut collected_subscriptions, e| match e {
                    NetworkBehaviourAction::NotifyHandler { event, .. } => {
                        for s in &event.subscriptions {
                            match s.action {
                                GossipsubSubscriptionAction::Subscribe => {
                                    collected_subscriptions.push(s.clone())
                                }
                                _ => {}
                            };
                        }
                        collected_subscriptions
                    }
                    _ => collected_subscriptions,
                });

        // we sent a subscribe to all known peers
        assert!(
            subscriptions.len() == 20,
            "Should send a subscription to all known peers"
        );
    }

    #[test]
    /// Test unsubscribe.
    fn test_unsubscribe() {
        // Unsubscribe should:
        // - Remove the mesh entry for topic
        // - Send UNSUBSCRIBE to all known peers
        // - Call Leave

        let topic_strings = vec![String::from("topic1"), String::from("topic2")];
        let topics = topic_strings
            .iter()
            .map(|t| Topic::new(t.clone()))
            .collect::<Vec<Topic>>();

        // subscribe to topic_strings
        let (mut gs, _, topic_hashes) = build_and_inject_nodes(20, topic_strings, true);

        for topic_hash in &topic_hashes {
            assert!(
                gs.topic_peers.get(&topic_hash).is_some(),
                "Topic_peers contain a topic entry"
            );
            assert!(
                gs.mesh.get(&topic_hash).is_some(),
                "mesh should contain a topic entry"
            );
        }

        // unsubscribe from both topics
        assert!(
            gs.unsubscribe(topics[0].clone()),
            "should be able to unsubscribe successfully from each topic",
        );
        assert!(
            gs.unsubscribe(topics[1].clone()),
            "should be able to unsubscribe successfully from each topic",
        );

        let subscriptions =
            gs.events
                .iter()
                .fold(vec![], |mut collected_subscriptions, e| match e {
                    NetworkBehaviourAction::NotifyHandler { event, .. } => {
                        for s in &event.subscriptions {
                            match s.action {
                                GossipsubSubscriptionAction::Unsubscribe => {
                                    collected_subscriptions.push(s.clone())
                                }
                                _ => {}
                            };
                        }
                        collected_subscriptions
                    }
                    _ => collected_subscriptions,
                });

        // we sent a unsubscribe to all known peers, for two topics
        assert!(
            subscriptions.len() == 40,
            "Should send an unsubscribe event to all known peers"
        );

        // check we clean up internal structures
        for topic_hash in &topic_hashes {
            assert!(
                gs.mesh.get(&topic_hash).is_none(),
                "All topics should have been removed from the mesh"
            );
        }
    }

    #[test]
    /// Test JOIN(topic) functionality.
    fn test_join() {
        // The Join function should:
        // - Remove peers from fanout[topic]
        // - Add any fanout[topic] peers to the mesh (up to mesh_n)
        // - Fill up to mesh_n peers from known gossipsub peers in the topic
        // - Send GRAFT messages to all nodes added to the mesh

        // This test is not an isolated unit test, rather it uses higher level,
        // subscribe/unsubscribe to perform the test.

        let topic_strings = vec![String::from("topic1"), String::from("topic2")];
        let topics = topic_strings
            .iter()
            .map(|t| Topic::new(t.clone()))
            .collect::<Vec<Topic>>();

        let (mut gs, _, topic_hashes) = build_and_inject_nodes(20, topic_strings, true);

        // unsubscribe, then call join to invoke functionality
        assert!(
            gs.unsubscribe(topics[0].clone()),
            "should be able to unsubscribe successfully"
        );
        assert!(
            gs.unsubscribe(topics[1].clone()),
            "should be able to unsubscribe successfully"
        );

        // re-subscribe - there should be peers associated with the topic
        assert!(
            gs.subscribe(topics[0].clone()),
            "should be able to subscribe successfully"
        );

        // should have added mesh_n nodes to the mesh
        assert!(
            gs.mesh.get(&topic_hashes[0]).unwrap().len() == 6,
            "Should have added 6 nodes to the mesh"
        );

        fn collect_grafts(
            mut collected_grafts: Vec<GossipsubControlAction>,
            (_, controls): (&PeerId, &Vec<GossipsubControlAction>),
        ) -> Vec<GossipsubControlAction> {
            for c in controls.iter() {
                match c {
                    GossipsubControlAction::Graft { topic_hash: _ } => {
                        collected_grafts.push(c.clone())
                    }
                    _ => {}
                }
            }
            collected_grafts
        }

        // there should be mesh_n GRAFT messages.
        let graft_messages = gs.control_pool.iter().fold(vec![], collect_grafts);

        assert_eq!(
            graft_messages.len(),
            6,
            "There should be 6 grafts messages sent to peers"
        );

        // verify fanout nodes
        // add 3 random peers to the fanout[topic1]
        gs.fanout
            .insert(topic_hashes[1].clone(), Default::default());
        let new_peers: Vec<PeerId> = vec![];
        for _ in 0..3 {
            let fanout_peers = gs.fanout.get_mut(&topic_hashes[1]).unwrap();
            fanout_peers.insert(PeerId::random());
        }

        // subscribe to topic1
        gs.subscribe(topics[1].clone());

        // the three new peers should have been added, along with 3 more from the pool.
        assert!(
            gs.mesh.get(&topic_hashes[1]).unwrap().len() == 6,
            "Should have added 6 nodes to the mesh"
        );
        let mesh_peers = gs.mesh.get(&topic_hashes[1]).unwrap();
        for new_peer in new_peers {
            assert!(
                mesh_peers.contains(&new_peer),
                "Fanout peer should be included in the mesh"
            );
        }

        // there should now be 12 graft messages to be sent
        let graft_messages = gs.control_pool.iter().fold(vec![], collect_grafts);

        assert!(
            graft_messages.len() == 12,
            "There should be 12 grafts messages sent to peers"
        );
    }

    /// Test local node publish to subscribed topic
    #[test]
    fn test_publish_without_flood_publishing() {
        // node should:
        // - Send publish message to all peers
        // - Insert message into gs.mcache and gs.received

        //turn off flood publish to test old behaviour
        let config = GossipsubConfigBuilder::new().flood_publish(false).build();

        let publish_topic = String::from("test_publish");
        let (mut gs, _, topic_hashes) =
            build_and_inject_nodes_with_config(20, vec![publish_topic.clone()], true, config);

        assert!(
            gs.mesh.get(&topic_hashes[0]).is_some(),
            "Subscribe should add a new entry to the mesh[topic] hashmap"
        );

        // all peers should be subscribed to the topic
        assert_eq!(
            gs.topic_peers.get(&topic_hashes[0]).map(|p| p.len()),
            Some(20),
            "Peers should be subscribed to the topic"
        );

        // publish on topic
        let publish_data = vec![0; 42];
        gs.publish(Topic::new(publish_topic), publish_data).unwrap();

        // Collect all publish messages
        let publishes = gs
            .events
            .iter()
            .fold(vec![], |mut collected_publish, e| match e {
                NetworkBehaviourAction::NotifyHandler { event, .. } => {
                    for s in &event.messages {
                        collected_publish.push(s.clone());
                    }
                    collected_publish
                }
                _ => collected_publish,
            });

        let msg_id =
            (gs.config.message_id_fn)(&publishes.first().expect("Should contain > 0 entries"));

        let config = GossipsubConfig::default();
        assert_eq!(
            publishes.len(),
            config.mesh_n_low,
            "Should send a publish message to all known peers"
        );

        assert!(
            gs.mcache.get(&msg_id).is_some(),
            "Message cache should contain published message"
        );
    }

    /// Test local node publish to unsubscribed topic
    #[test]
    fn test_fanout() {
        // node should:
        // - Populate fanout peers
        // - Send publish message to fanout peers
        // - Insert message into gs.mcache and gs.received

        //turn off flood publish to test fanout behaviour
        let config = GossipsubConfigBuilder::new().flood_publish(false).build();

        let fanout_topic = String::from("test_fanout");
        let (mut gs, _, topic_hashes) =
            build_and_inject_nodes_with_config(20, vec![fanout_topic.clone()], true, config);

        assert!(
            gs.mesh.get(&topic_hashes[0]).is_some(),
            "Subscribe should add a new entry to the mesh[topic] hashmap"
        );
        // Unsubscribe from topic
        assert!(
            gs.unsubscribe(Topic::new(fanout_topic.clone())),
            "should be able to unsubscribe successfully from topic"
        );

        // Publish on unsubscribed topic
        let publish_data = vec![0; 42];
        gs.publish(Topic::new(fanout_topic.clone()), publish_data)
            .unwrap();

        assert_eq!(
            gs.fanout
                .get(&TopicHash::from_raw(fanout_topic.clone()))
                .unwrap()
                .len(),
            gs.config.mesh_n,
            "Fanout should contain `mesh_n` peers for fanout topic"
        );

        // Collect all publish messages
        let publishes = gs
            .events
            .iter()
            .fold(vec![], |mut collected_publish, e| match e {
                NetworkBehaviourAction::NotifyHandler { event, .. } => {
                    for s in &event.messages {
                        collected_publish.push(s.clone());
                    }
                    collected_publish
                }
                _ => collected_publish,
            });

        let msg_id =
            (gs.config.message_id_fn)(&publishes.first().expect("Should contain > 0 entries"));

        assert_eq!(
            publishes.len(),
            gs.config.mesh_n,
            "Should send a publish message to `mesh_n` fanout peers"
        );

        assert!(
            gs.mcache.get(&msg_id).is_some(),
            "Message cache should contain published message"
        );
    }

    #[test]
    /// Test the gossipsub NetworkBehaviour peer connection logic.
    fn test_inject_connected() {
        let (gs, peers, topic_hashes) = build_and_inject_nodes(
            20,
            vec![String::from("topic1"), String::from("topic2")],
            true,
        );

        // check that our subscriptions are sent to each of the peers
        // collect all the SendEvents
        let send_events: Vec<&NetworkBehaviourAction<Arc<GossipsubRpc>, GossipsubEvent>> = gs
            .events
            .iter()
            .filter(|e| match e {
                NetworkBehaviourAction::NotifyHandler { event, .. } => {
                    !event.subscriptions.is_empty()
                }
                _ => false,
            })
            .collect();

        // check that there are two subscriptions sent to each peer
        for sevent in send_events.clone() {
            match sevent {
                NetworkBehaviourAction::NotifyHandler { event, .. } => {
                    assert!(
                        event.subscriptions.len() == 2,
                        "There should be two subscriptions sent to each peer (1 for each topic)."
                    );
                }
                _ => {}
            };
        }

        // check that there are 20 send events created
        assert!(
            send_events.len() == 20,
            "There should be a subscription event sent to each peer."
        );

        // should add the new peers to `peer_topics` with an empty vec as a gossipsub node
        for peer in peers {
            let known_topics = gs.peer_topics.get(&peer).unwrap();
            assert!(
                known_topics == &topic_hashes.iter().cloned().collect(),
                "The topics for each node should all topics"
            );
        }
    }

    #[test]
    /// Test subscription handling
    fn test_handle_received_subscriptions() {
        // For every subscription:
        // SUBSCRIBE:   - Add subscribed topic to peer_topics for peer.
        //              - Add peer to topics_peer.
        // UNSUBSCRIBE  - Remove topic from peer_topics for peer.
        //              - Remove peer from topic_peers.

        let topics = vec!["topic1", "topic2", "topic3", "topic4"]
            .iter()
            .map(|&t| String::from(t))
            .collect();
        let (mut gs, peers, topic_hashes) = build_and_inject_nodes(20, topics, false);

        // The first peer sends 3 subscriptions and 1 unsubscription
        let mut subscriptions = topic_hashes[..3]
            .iter()
            .map(|topic_hash| GossipsubSubscription {
                action: GossipsubSubscriptionAction::Subscribe,
                topic_hash: topic_hash.clone(),
            })
            .collect::<Vec<GossipsubSubscription>>();

        subscriptions.push(GossipsubSubscription {
            action: GossipsubSubscriptionAction::Unsubscribe,
            topic_hash: topic_hashes[topic_hashes.len() - 1].clone(),
        });

        let unknown_peer = PeerId::random();
        // process the subscriptions
        // first and second peers send subscriptions
        gs.handle_received_subscriptions(&subscriptions, &peers[0]);
        gs.handle_received_subscriptions(&subscriptions, &peers[1]);
        // unknown peer sends the same subscriptions
        gs.handle_received_subscriptions(&subscriptions, &unknown_peer);

        // verify the result

        let peer_topics = gs.peer_topics.get(&peers[0]).unwrap().clone();
        assert!(
            peer_topics == topic_hashes.iter().take(3).cloned().collect(),
            "First peer should be subscribed to three topics"
        );
        let peer_topics = gs.peer_topics.get(&peers[1]).unwrap().clone();
        assert!(
            peer_topics == topic_hashes.iter().take(3).cloned().collect(),
            "Second peer should be subscribed to three topics"
        );

        assert!(
            gs.peer_topics.get(&unknown_peer).is_none(),
            "Unknown peer should not have been added"
        );

        for topic_hash in topic_hashes[..3].iter() {
            let topic_peers = gs.topic_peers.get(topic_hash).unwrap().clone();
            assert!(
                topic_peers == peers[..2].into_iter().cloned().collect(),
                "Two peers should be added to the first three topics"
            );
        }

        // Peer 0 unsubscribes from the first topic

        gs.handle_received_subscriptions(
            &vec![GossipsubSubscription {
                action: GossipsubSubscriptionAction::Unsubscribe,
                topic_hash: topic_hashes[0].clone(),
            }],
            &peers[0],
        );

        let peer_topics = gs.peer_topics.get(&peers[0]).unwrap().clone();
        assert!(
            peer_topics == topic_hashes[1..3].into_iter().cloned().collect(),
            "Peer should be subscribed to two topics"
        );

        let topic_peers = gs.topic_peers.get(&topic_hashes[0]).unwrap().clone(); // only gossipsub at the moment
        assert!(
            topic_peers == peers[1..2].into_iter().cloned().collect(),
            "Only the second peers should be in the first topic"
        );
    }

    #[test]
    /// Test Gossipsub.get_random_peers() function
    fn test_get_random_peers() {
        // generate a default GossipsubConfig
        let mut gs_config = GossipsubConfig::default();
        gs_config.validation_mode = ValidationMode::Anonymous;
        // create a gossipsub struct
        let mut gs: Gossipsub = Gossipsub::new(MessageAuthenticity::Anonymous, gs_config);

        // create a topic and fill it with some peers
        let topic_hash = Topic::new("Test").hash().clone();
        let mut peers = vec![];
        for _ in 0..20 {
            peers.push(PeerId::random())
        }

        gs.topic_peers
            .insert(topic_hash.clone(), peers.iter().cloned().collect());

        let random_peers = Gossipsub::get_random_peers(&gs.topic_peers, &topic_hash, 5, |_| true);
        assert_eq!(random_peers.len(), 5, "Expected 5 peers to be returned");
        let random_peers = Gossipsub::get_random_peers(&gs.topic_peers, &topic_hash, 30, |_| true);
        assert!(random_peers.len() == 20, "Expected 20 peers to be returned");
        assert!(
            random_peers == peers.iter().cloned().collect(),
            "Expected no shuffling"
        );
        let random_peers = Gossipsub::get_random_peers(&gs.topic_peers, &topic_hash, 20, |_| true);
        assert!(random_peers.len() == 20, "Expected 20 peers to be returned");
        assert!(
            random_peers == peers.iter().cloned().collect(),
            "Expected no shuffling"
        );
        let random_peers = Gossipsub::get_random_peers(&gs.topic_peers, &topic_hash, 0, |_| true);
        assert!(random_peers.len() == 0, "Expected 0 peers to be returned");
        // test the filter
        let random_peers = Gossipsub::get_random_peers(&gs.topic_peers, &topic_hash, 5, |_| false);
        assert!(random_peers.len() == 0, "Expected 0 peers to be returned");
        let random_peers = Gossipsub::get_random_peers(&gs.topic_peers, &topic_hash, 10, {
            |peer| peers.contains(peer)
        });
        assert!(random_peers.len() == 10, "Expected 10 peers to be returned");
    }

    /// Tests that the correct message is sent when a peer asks for a message in our cache.
    #[test]
    fn test_handle_iwant_msg_cached() {
        let (mut gs, peers, _) = build_and_inject_nodes(20, Vec::new(), true);

        let id = gs.config.message_id_fn;

        let message = GossipsubMessage {
            source: Some(peers[11].clone()),
            data: vec![1, 2, 3, 4],
            sequence_number: Some(1u64),
            topics: Vec::new(),
            signature: None,
            key: None,
            validated: true,
        };
        let msg_id = id(&message);
        gs.mcache.put(message.clone());

        gs.handle_iwant(&peers[7], vec![msg_id.clone()]);

        // the messages we are sending
        let sent_messages = gs
            .events
            .iter()
            .fold(vec![], |mut collected_messages, e| match e {
                NetworkBehaviourAction::NotifyHandler { event, .. } => {
                    for c in &event.messages {
                        collected_messages.push(c.clone())
                    }
                    collected_messages
                }
                _ => collected_messages,
            });

        assert!(
            sent_messages.iter().any(|msg| id(msg) == msg_id),
            "Expected the cached message to be sent to an IWANT peer"
        );
    }

    /// Tests that messages are sent correctly depending on the shifting of the message cache.
    #[test]
    fn test_handle_iwant_msg_cached_shifted() {
        let (mut gs, peers, _) = build_and_inject_nodes(20, Vec::new(), true);

        let id = gs.config.message_id_fn;
        // perform 10 memshifts and check that it leaves the cache
        for shift in 1..10 {
            let message = GossipsubMessage {
                source: Some(peers[11].clone()),
                data: vec![1, 2, 3, 4],
                sequence_number: Some(shift),
                topics: Vec::new(),
                signature: None,
                key: None,
                validated: true,
            };
            let msg_id = id(&message);
            gs.mcache.put(message.clone());
            for _ in 0..shift {
                gs.mcache.shift();
            }

            gs.handle_iwant(&peers[7], vec![msg_id.clone()]);

            // is the message is being sent?
            let message_exists = gs.events.iter().any(|e| match e {
                NetworkBehaviourAction::NotifyHandler { event, .. } => {
                    event.messages.iter().any(|msg| id(msg) == msg_id)
                }
                _ => false,
            });
            // default history_length is 5, expect no messages after shift > 5
            if shift < 5 {
                assert!(
                    message_exists,
                    "Expected the cached message to be sent to an IWANT peer before 5 shifts"
                );
            } else {
                assert!(
                    !message_exists,
                    "Expected the cached message to not be sent to an IWANT peer after 5 shifts"
                );
            }
        }
    }

    #[test]
    // tests that an event is not created when a peers asks for a message not in our cache
    fn test_handle_iwant_msg_not_cached() {
        let (mut gs, peers, _) = build_and_inject_nodes(20, Vec::new(), true);

        let events_before = gs.events.len();
        gs.handle_iwant(&peers[7], vec![MessageId::new(b"unknown id")]);
        let events_after = gs.events.len();

        assert_eq!(
            events_before, events_after,
            "Expected event count to stay the same"
        );
    }

    #[test]
    // tests that an event is created when a peer shares that it has a message we want
    fn test_handle_ihave_subscribed_and_msg_not_cached() {
        let (mut gs, peers, topic_hashes) =
            build_and_inject_nodes(20, vec![String::from("topic1")], true);

        gs.handle_ihave(
            &peers[7],
            vec![(topic_hashes[0].clone(), vec![MessageId::new(b"unknown id")])],
        );

        // check that we sent an IWANT request for `unknown id`
        let iwant_exists = match gs.control_pool.get(&peers[7]) {
            Some(controls) => controls.iter().any(|c| match c {
                GossipsubControlAction::IWant { message_ids } => message_ids
                    .iter()
                    .any(|m| *m == MessageId::new(b"unknown id")),
                _ => false,
            }),
            _ => false,
        };

        assert!(
            iwant_exists,
            "Expected to send an IWANT control message for unkown message id"
        );
    }

    #[test]
    // tests that an event is not created when a peer shares that it has a message that
    // we already have
    fn test_handle_ihave_subscribed_and_msg_cached() {
        let (mut gs, peers, topic_hashes) =
            build_and_inject_nodes(20, vec![String::from("topic1")], true);

        let msg_id = MessageId::new(b"known id");

        let events_before = gs.events.len();
        gs.handle_ihave(&peers[7], vec![(topic_hashes[0].clone(), vec![msg_id])]);
        let events_after = gs.events.len();

        assert_eq!(
            events_before, events_after,
            "Expected event count to stay the same"
        )
    }

    #[test]
    // test that an event is not created when a peer shares that it has a message in
    // a topic that we are not subscribed to
    fn test_handle_ihave_not_subscribed() {
        let (mut gs, peers, _) = build_and_inject_nodes(20, vec![], true);

        let events_before = gs.events.len();
        gs.handle_ihave(
            &peers[7],
            vec![(
                TopicHash::from_raw(String::from("unsubscribed topic")),
                vec![MessageId::new(b"irrelevant id")],
            )],
        );
        let events_after = gs.events.len();

        assert_eq!(
            events_before, events_after,
            "Expected event count to stay the same"
        )
    }

    #[test]
    // tests that a peer is added to our mesh when we are both subscribed
    // to the same topic
    fn test_handle_graft_is_subscribed() {
        let (mut gs, peers, topic_hashes) =
            build_and_inject_nodes(20, vec![String::from("topic1")], true);

        gs.handle_graft(&peers[7], topic_hashes.clone());

        assert!(
            gs.mesh.get(&topic_hashes[0]).unwrap().contains(&peers[7]),
            "Expected peer to have been added to mesh"
        );
    }

    #[test]
    // tests that a peer is not added to our mesh when they are subscribed to
    // a topic that we are not
    fn test_handle_graft_is_not_subscribed() {
        let (mut gs, peers, topic_hashes) =
            build_and_inject_nodes(20, vec![String::from("topic1")], true);

        gs.handle_graft(
            &peers[7],
            vec![TopicHash::from_raw(String::from("unsubscribed topic"))],
        );

        assert!(
            !gs.mesh.get(&topic_hashes[0]).unwrap().contains(&peers[7]),
            "Expected peer to have been added to mesh"
        );
    }

    #[test]
    // tests multiple topics in a single graft message
    fn test_handle_graft_multiple_topics() {
        let topics: Vec<String> = vec!["topic1", "topic2", "topic3", "topic4"]
            .iter()
            .map(|&t| String::from(t))
            .collect();

        let (mut gs, peers, topic_hashes) = build_and_inject_nodes(20, topics.clone(), true);

        let mut their_topics = topic_hashes.clone();
        // their_topics = [topic1, topic2, topic3]
        // our_topics = [topic1, topic2, topic4]
        their_topics.pop();
        gs.leave(&their_topics[2]);

        gs.handle_graft(&peers[7], their_topics.clone());

        for i in 0..2 {
            assert!(
                gs.mesh.get(&topic_hashes[i]).unwrap().contains(&peers[7]),
                "Expected peer to be in the mesh for the first 2 topics"
            );
        }

        assert!(
            gs.mesh.get(&topic_hashes[2]).is_none(),
            "Expected the second topic to not be in the mesh"
        );
    }

    #[test]
    // tests that a peer is removed from our mesh
    fn test_handle_prune_peer_in_mesh() {
        let (mut gs, peers, topic_hashes) =
            build_and_inject_nodes(20, vec![String::from("topic1")], true);

        // insert peer into our mesh for 'topic1'
        gs.mesh
            .insert(topic_hashes[0].clone(), peers.iter().cloned().collect());
        assert!(
            gs.mesh.get(&topic_hashes[0]).unwrap().contains(&peers[7]),
            "Expected peer to be in mesh"
        );

        gs.handle_prune(
            &peers[7],
            topic_hashes
                .iter()
                .map(|h| (h.clone(), vec![], None))
                .collect(),
        );
        assert!(
            !gs.mesh.get(&topic_hashes[0]).unwrap().contains(&peers[7]),
            "Expected peer to be removed from mesh"
        );
    }

    fn count_control_msgs(
        gs: &Gossipsub,
        mut filter: impl FnMut(&PeerId, &GossipsubControlAction) -> bool,
    ) -> usize {
        gs.control_pool
            .iter()
            .map(|(peer_id, actions)| actions.iter().filter(|m| filter(peer_id, m)).count())
            .sum::<usize>()
            + gs.events
                .iter()
                .map(|e| match e {
                    NetworkBehaviourAction::NotifyHandler { peer_id, event, .. } => event
                        .control_msgs
                        .iter()
                        .filter(|m| filter(peer_id, m))
                        .count(),
                    _ => 0,
                })
                .sum::<usize>()
    }

    fn flush_events(gs: &mut Gossipsub) {
        gs.control_pool.clear();
        gs.events.clear();
    }

    #[test]
    // tests that a peer added as explicit peer gets connected to
    fn test_explicit_peer_gets_connected() {
        let (mut gs, _, _) = build_and_inject_nodes(0, Vec::new(), true);

        //create new peer
        let peer = PeerId::random();

        //add peer as explicit peer
        gs.add_explicit_peer(&peer);

        let dial_events: Vec<&NetworkBehaviourAction<Arc<GossipsubRpc>, GossipsubEvent>> = gs
            .events
            .iter()
            .filter(|e| match e {
                NetworkBehaviourAction::DialPeer {
                    peer_id,
                    condition: DialPeerCondition::Disconnected,
                } => peer_id == &peer,
                _ => false,
            })
            .collect();

        assert_eq!(
            dial_events.len(),
            1,
            "There was no dial peer event for the explicit peer"
        );
    }

    #[test]
    fn test_explicit_peer_reconnects() {
        let config = GossipsubConfigBuilder::new()
            .check_explicit_peers_ticks(2)
            .build();
        let (mut gs, others, _) = build_and_inject_nodes_with_config(1, Vec::new(), true, config);

        let peer = others.get(0).unwrap();

        //add peer as explicit peer
        gs.add_explicit_peer(peer);

        flush_events(&mut gs);

        //disconnect peer
        gs.inject_disconnected(peer);

        gs.heartbeat();

        //check that no reconnect after first heartbeat since `explicit_peer_ticks == 2`
        assert_eq!(
            gs.events
                .iter()
                .filter(|e| match e {
                    NetworkBehaviourAction::DialPeer {
                        peer_id,
                        condition: DialPeerCondition::Disconnected,
                    } => peer_id == peer,
                    _ => false,
                })
                .count(),
            0,
            "There was a dial peer event before explicit_peer_ticks heartbeats"
        );

        gs.heartbeat();

        //check that there is a reconnect after second heartbeat
        assert!(
            gs.events
                .iter()
                .filter(|e| match e {
                    NetworkBehaviourAction::DialPeer {
                        peer_id,
                        condition: DialPeerCondition::Disconnected,
                    } => peer_id == peer,
                    _ => false,
                })
                .count()
                >= 1,
            "There was no dial peer event for the explicit peer"
        );
    }

    #[test]
    fn test_handle_graft_explicit_peer() {
        let (mut gs, peers, topic_hashes) = build_and_inject_nodes_with_config_and_explicit(
            1,
            vec![String::from("topic1"), String::from("topic2")],
            true,
            GossipsubConfig::default(),
            1,
        );

        let peer = peers.get(0).unwrap();

        gs.handle_graft(peer, topic_hashes.clone());

        //peer got not added to mesh
        assert!(gs.mesh[&topic_hashes[0]].is_empty());
        assert!(gs.mesh[&topic_hashes[1]].is_empty());

        //check prunes
        assert!(
            count_control_msgs(&gs, |peer_id, m| peer_id == peer
                && match m {
                    GossipsubControlAction::Prune { topic_hash, .. } =>
                        topic_hash == &topic_hashes[0] || topic_hash == &topic_hashes[1],
                    _ => false,
                })
                >= 2,
            "Not enough prunes sent when grafting from explicit peer"
        );
    }

    #[test]
    fn explicit_peers_not_added_to_mesh_on_receiving_subscription() {
        let (gs, peers, topic_hashes) = build_and_inject_nodes_with_config_and_explicit(
            2,
            vec![String::from("topic1")],
            true,
            GossipsubConfig::default(),
            1,
        );

        //only peer 1 is in the mesh not peer 0 (which is an explicit peer)
        assert_eq!(
            gs.mesh[&topic_hashes[0]],
            vec![peers[1].clone()].into_iter().collect()
        );

        //assert that graft gets created to non-explicit peer
        assert!(
            count_control_msgs(&gs, |peer_id, m| peer_id == &peers[1]
                && match m {
                    GossipsubControlAction::Graft { .. } => true,
                    _ => false,
                })
                >= 1,
            "No graft message got created to non-explicit peer"
        );

        //assert that no graft gets created to explicit peer
        assert_eq!(
            count_control_msgs(&gs, |peer_id, m| peer_id == &peers[0]
                && match m {
                    GossipsubControlAction::Graft { .. } => true,
                    _ => false,
                }),
            0,
            "A graft message got created to an explicit peer"
        );
    }

    #[test]
    fn do_not_graft_explicit_peer() {
        let (mut gs, others, topic_hashes) = build_and_inject_nodes_with_config_and_explicit(
            1,
            vec![String::from("topic")],
            true,
            GossipsubConfig::default(),
            1,
        );

        gs.heartbeat();

        //mesh stays empty
        assert_eq!(gs.mesh[&topic_hashes[0]], BTreeSet::new());

        //assert that no graft gets created to explicit peer
        assert_eq!(
            count_control_msgs(&gs, |peer_id, m| peer_id == &others[0]
                && match m {
                    GossipsubControlAction::Graft { .. } => true,
                    _ => false,
                }),
            0,
            "A graft message got created to an explicit peer"
        );
    }

    #[test]
    fn do_forward_messages_to_explicit_peers() {
        let (mut gs, peers, topic_hashes) = build_and_inject_nodes_with_config_and_explicit(
            2,
            vec![String::from("topic1"), String::from("topic2")],
            true,
            GossipsubConfig::default(),
            1,
        );

        let local_id = PeerId::random();

        let message = GossipsubMessage {
            source: Some(peers[1].clone()),
            data: vec![],
            sequence_number: Some(0),
            topics: vec![topic_hashes[0].clone()],
            signature: None,
            key: None,
            validated: true,
        };
        gs.handle_received_message(message.clone(), &local_id);

        assert_eq!(
            gs.events
                .iter()
                .filter(|e| match e {
                    NetworkBehaviourAction::NotifyHandler { peer_id, event, .. } =>
                        peer_id == &peers[0]
                            && event.messages.iter().filter(|m| *m == &message).count() > 0,
                    _ => false,
                })
                .count(),
            1,
            "The message did not get forwarded to the explicit peer"
        );
    }

    #[test]
    fn explicit_peers_not_added_to_mesh_on_subscribe() {
        let (mut gs, peers, _) = build_and_inject_nodes_with_config_and_explicit(
            2,
            Vec::new(),
            true,
            GossipsubConfig::default(),
            1,
        );

        //create new topic, both peers subscribing to it but we do not subscribe to it
        let topic = Topic::new(String::from("t"));
        let topic_hash = topic.hash();
        for i in 0..2 {
            gs.handle_received_subscriptions(
                &vec![GossipsubSubscription {
                    action: GossipsubSubscriptionAction::Subscribe,
                    topic_hash: topic_hash.clone(),
                }],
                &peers[i],
            );
        }

        //subscribe now to topic
        gs.subscribe(topic.clone());

        //only peer 1 is in the mesh not peer 0 (which is an explicit peer)
        assert_eq!(
            gs.mesh[&topic_hash],
            vec![peers[1].clone()].into_iter().collect()
        );

        //assert that graft gets created to non-explicit peer
        assert!(
            count_control_msgs(&gs, |peer_id, m| peer_id == &peers[1]
                && match m {
                    GossipsubControlAction::Graft { .. } => true,
                    _ => false,
                })
                > 0,
            "No graft message got created to non-explicit peer"
        );

        //assert that no graft gets created to explicit peer
        assert_eq!(
            count_control_msgs(&gs, |peer_id, m| peer_id == &peers[0]
                && match m {
                    GossipsubControlAction::Graft { .. } => true,
                    _ => false,
                }),
            0,
            "A graft message got created to an explicit peer"
        );
    }

    #[test]
    fn explicit_peers_not_added_to_mesh_from_fanout_on_subscribe() {
        let (mut gs, peers, _) = build_and_inject_nodes_with_config_and_explicit(
            2,
            Vec::new(),
            true,
            GossipsubConfig::default(),
            1,
        );

        //create new topic, both peers subscribing to it but we do not subscribe to it
        let topic = Topic::new(String::from("t"));
        let topic_hash = topic.hash();
        for i in 0..2 {
            gs.handle_received_subscriptions(
                &vec![GossipsubSubscription {
                    action: GossipsubSubscriptionAction::Subscribe,
                    topic_hash: topic_hash.clone(),
                }],
                &peers[i],
            );
        }

        //we send a message for this topic => this will initialize the fanout
        gs.publish(topic.clone(), vec![1, 2, 3]).unwrap();

        //subscribe now to topic
        gs.subscribe(topic.clone());

        //only peer 1 is in the mesh not peer 0 (which is an explicit peer)
        assert_eq!(
            gs.mesh[&topic_hash],
            vec![peers[1].clone()].into_iter().collect()
        );

        //assert that graft gets created to non-explicit peer
        assert!(
            count_control_msgs(&gs, |peer_id, m| peer_id == &peers[1]
                && match m {
                    GossipsubControlAction::Graft { .. } => true,
                    _ => false,
                })
                >= 1,
            "No graft message got created to non-explicit peer"
        );

        //assert that no graft gets created to explicit peer
        assert_eq!(
            count_control_msgs(&gs, |peer_id, m| peer_id == &peers[0]
                && match m {
                    GossipsubControlAction::Graft { .. } => true,
                    _ => false,
                }),
            0,
            "A graft message got created to an explicit peer"
        );
    }

    #[test]
    fn no_gossip_gets_sent_to_explicit_peers() {
        let (mut gs, peers, topic_hashes) = build_and_inject_nodes_with_config_and_explicit(
            2,
            vec![String::from("topic1"), String::from("topic2")],
            true,
            GossipsubConfig::default(),
            1,
        );

        let local_id = PeerId::random();

        let message = GossipsubMessage {
            source: Some(peers[1].clone()),
            data: vec![],
            sequence_number: Some(0),
            topics: vec![topic_hashes[0].clone()],
            signature: None,
            key: None,
            validated: true,
        };

        //forward the message
        gs.handle_received_message(message.clone(), &local_id);

        //simulate multiple gossip calls (for randomness)
        for _ in 0..3 {
            gs.emit_gossip();
        }

        //assert that no gossip gets sent to explicit peer
        assert_eq!(
            gs.control_pool
                .get(&peers[0])
                .unwrap_or(&Vec::new())
                .iter()
                .filter(|m| match m {
                    GossipsubControlAction::IHave { .. } => true,
                    _ => false,
                })
                .count(),
            0,
            "Gossip got emitted to explicit peer"
        );
    }

    #[test]
    // Tests the mesh maintenance addition
    fn test_mesh_addition() {
        let config = GossipsubConfig::default();

        // Adds mesh_low peers and PRUNE 2 giving us a deficit.
        let (mut gs, peers, topics) =
            build_and_inject_nodes(config.mesh_n + 1, vec!["test".into()], true);

        let to_remove_peers = config.mesh_n + 1 - config.mesh_n_low - 1;

        for index in 0..to_remove_peers {
            gs.handle_prune(
                &peers[index],
                topics.iter().map(|h| (h.clone(), vec![], None)).collect(),
            );
        }

        // Verify the pruned peers are removed from the mesh.
        assert_eq!(
            gs.mesh.get(&topics[0]).unwrap().len(),
            config.mesh_n_low - 1
        );

        // run a heartbeat
        gs.heartbeat();

        // Peers should be added to reach mesh_n
        assert_eq!(gs.mesh.get(&topics[0]).unwrap().len(), config.mesh_n);
    }

    #[test]
    // Tests the mesh maintenance subtraction
    fn test_mesh_subtraction() {
        let config = GossipsubConfig::default();

        // Adds mesh_low peers and PRUNE 2 giving us a deficit.
        let (mut gs, peers, topics) =
            build_and_inject_nodes(config.mesh_n_high + 10, vec!["test".into()], true);

        // graft all the peers
        for peer in peers {
            gs.handle_graft(&peer, topics.clone());
        }

        // run a heartbeat
        gs.heartbeat();

        // Peers should be removed to reach mesh_n
        assert_eq!(gs.mesh.get(&topics[0]).unwrap().len(), config.mesh_n);
    }

    #[test]
    fn test_connect_to_px_peers_on_handle_prune() {
        let config = GossipsubConfig::default();

        let (mut gs, peers, topics) = build_and_inject_nodes(1, vec!["test".into()], true);

        //handle prune from single peer with px peers

        let mut px = Vec::new();
        //propose more px peers than config.prune_peers
        for _ in 0..config.prune_peers + 5 {
            px.push(PeerInfo {
                peer: Some(PeerId::random()),
            });
        }

        gs.handle_prune(
            &peers[0],
            vec![(
                topics[0].clone(),
                px.clone(),
                Some(config.prune_backoff.as_secs()),
            )],
        );

        //Check DialPeer events for px peers
        let dials: Vec<_> = gs
            .events
            .iter()
            .filter_map(|e| match e {
                NetworkBehaviourAction::DialPeer {
                    peer_id,
                    condition: DialPeerCondition::Disconnected,
                } => Some(peer_id.clone()),
                _ => None,
            })
            .collect();

        // Exactly config.prune_peers many random peers should be dialled
        assert_eq!(dials.len(), config.prune_peers);

        let dials_set: HashSet<_> = dials.into_iter().collect();

        // No duplicates
        assert_eq!(dials_set.len(), config.prune_peers);

        //all dial peers must be in px
        assert!(dials_set.is_subset(&HashSet::from_iter(
            px.iter().map(|i| i.peer.as_ref().unwrap().clone())
        )));
    }

    #[test]
    fn test_send_px_and_backoff_in_prune() {
        let config = GossipsubConfig::default();

        //build mesh with enough peers for px
        let (mut gs, peers, topics) =
            build_and_inject_nodes(config.prune_peers + 1, vec!["test".into()], true);

        //send prune to peer
        gs.send_graft_prune(
            HashMap::new(),
            vec![(peers[0].clone(), vec![topics[0].clone()])]
                .into_iter()
                .collect(),
        );

        //check prune message
        assert_eq!(
            count_control_msgs(&gs, |peer_id, m| peer_id == &peers[0]
                && match m {
                    GossipsubControlAction::Prune {
                        topic_hash,
                        peers,
                        backoff,
                    } =>
                        topic_hash == &topics[0] &&
                peers.len() == config.prune_peers &&
                //all peers are different
                peers.iter().collect::<HashSet<_>>().len() ==
                    config.prune_peers &&
                backoff.unwrap() == config.prune_backoff.as_secs(),
                    _ => false,
                }),
            1
        );
    }

    #[test]
    fn test_prune_backoffed_peer_on_graft() {
        let config = GossipsubConfig::default();

        //build mesh with enough peers for px
        let (mut gs, peers, topics) =
            build_and_inject_nodes(config.prune_peers + 1, vec!["test".into()], true);

        //send prune to peer => this adds a backoff for this peer
        gs.send_graft_prune(
            HashMap::new(),
            vec![(peers[0].clone(), vec![topics[0].clone()])]
                .into_iter()
                .collect(),
        );

        //ignore all messages until now
        gs.events.clear();

        //handle graft
        gs.handle_graft(&peers[0], vec![topics[0].clone()]);

        //check prune message
        assert_eq!(
            count_control_msgs(&gs, |peer_id, m| peer_id == &peers[0]
                && match m {
                    GossipsubControlAction::Prune {
                        topic_hash,
                        peers,
                        backoff,
                    } =>
                        topic_hash == &topics[0] &&
                //no px in this case
                peers.is_empty() &&
                backoff.unwrap() == config.prune_backoff.as_secs(),
                    _ => false,
                }),
            1
        );
    }

    #[test]
    fn test_do_not_graft_within_backoff_period() {
        let config = GossipsubConfigBuilder::new()
            .backoff_slack(1)
            .heartbeat_interval(Duration::from_millis(100))
            .build();
        //only one peer => mesh too small and will try to regraft as early as possible
        let (mut gs, peers, topics) =
            build_and_inject_nodes_with_config(1, vec!["test".into()], true, config);

        //handle prune from peer with backoff of one second
        gs.handle_prune(&peers[0], vec![(topics[0].clone(), Vec::new(), Some(1))]);

        //forget all events until now
        flush_events(&mut gs);

        //call heartbeat
        gs.heartbeat();

        //Sleep for one second and apply 10 regular heartbeats (interval = 100ms).
        for _ in 0..10 {
            sleep(Duration::from_millis(100));
            gs.heartbeat();
        }

        //Check that no graft got created (we have backoff_slack = 1 therefore one more heartbeat
        // is needed).
        assert_eq!(
            count_control_msgs(&gs, |_, m| match m {
                GossipsubControlAction::Graft { .. } => true,
                _ => false,
            }),
            0,
            "Graft message created too early within backoff period"
        );

        //Heartbeat one more time this should graft now
        sleep(Duration::from_millis(100));
        gs.heartbeat();

        //check that graft got created
        assert!(
            count_control_msgs(&gs, |_, m| match m {
                GossipsubControlAction::Graft { .. } => true,
                _ => false,
            }) > 0,
            "No graft message was created after backoff period"
        );
    }

    #[test]
    fn test_do_not_graft_within_default_backoff_period_after_receiving_prune_without_backoff() {
        //set default backoff period to 1 second
        let config = GossipsubConfigBuilder::new()
            .prune_backoff(Duration::from_millis(90))
            .backoff_slack(1)
            .heartbeat_interval(Duration::from_millis(100))
            .build();
        //only one peer => mesh too small and will try to regraft as early as possible
        let (mut gs, peers, topics) =
            build_and_inject_nodes_with_config(1, vec!["test".into()], true, config);

        //handle prune from peer without a specified backoff
        gs.handle_prune(&peers[0], vec![(topics[0].clone(), Vec::new(), None)]);

        //forget all events until now
        flush_events(&mut gs);

        //call heartbeat
        gs.heartbeat();

        //Apply one more heartbeat
        sleep(Duration::from_millis(100));
        gs.heartbeat();

        //Check that no graft got created (we have backoff_slack = 1 therefore one more heartbeat
        // is needed).
        assert_eq!(
            count_control_msgs(&gs, |_, m| match m {
                GossipsubControlAction::Graft { .. } => true,
                _ => false,
            }),
            0,
            "Graft message created too early within backoff period"
        );

        //Heartbeat one more time this should graft now
        sleep(Duration::from_millis(100));
        gs.heartbeat();

        //check that graft got created
        assert!(
            count_control_msgs(&gs, |_, m| match m {
                GossipsubControlAction::Graft { .. } => true,
                _ => false,
            }) > 0,
            "No graft message was created after backoff period"
        );
    }

    #[test]
    fn test_flood_publish() {
        let config = GossipsubConfig::default();

        let topic = "test";
        // Adds more peers than mesh can hold to test flood publishing
        let (mut gs, _, _) =
            build_and_inject_nodes(config.mesh_n_high + 10, vec![topic.into()], true);

        let other_topic = Topic::new("test2");

        // subscribe an additional new peer to test2
        gs.subscribe(other_topic.clone());
        let other_peer = PeerId::random();
        gs.inject_connected(&other_peer);
        gs.handle_received_subscriptions(
            &vec![GossipsubSubscription {
                action: GossipsubSubscriptionAction::Subscribe,
                topic_hash: other_topic.hash(),
            }],
            &other_peer,
        );

        //publish message
        let publish_data = vec![0; 42];
        gs.publish_many(vec![Topic::new(topic), other_topic.clone()], publish_data)
            .unwrap();

        // Collect all publish messages
        let publishes = gs
            .events
            .iter()
            .fold(vec![], |mut collected_publish, e| match e {
                NetworkBehaviourAction::NotifyHandler { event, .. } => {
                    for s in &event.messages {
                        collected_publish.push(s.clone());
                    }
                    collected_publish
                }
                _ => collected_publish,
            });

        let msg_id =
            (gs.config.message_id_fn)(&publishes.first().expect("Should contain > 0 entries"));

        let config = GossipsubConfig::default();
        assert_eq!(
            publishes.len(),
            config.mesh_n_high + 10 + 1,
            "Should send a publish message to all known peers"
        );

        assert!(
            gs.mcache.get(&msg_id).is_some(),
            "Message cache should contain published message"
        );
    }

    #[test]
    fn test_gossip_to_at_least_gossip_lazy_peers() {
        let config = GossipsubConfig::default();

        //add more peers than in mesh to test gossipping
        //by default only mesh_n_low peers will get added to mesh
        let (mut gs, _, topic_hashes) = build_and_inject_nodes(
            config.mesh_n_low + config.gossip_lazy + 1,
            vec!["topic".into()],
            true,
        );

        //receive message
        let message = GossipsubMessage {
            source: Some(PeerId::random()),
            data: vec![],
            sequence_number: Some(0),
            topics: vec![topic_hashes[0].clone()],
            signature: None,
            key: None,
            validated: true,
        };
        gs.handle_received_message(message.clone(), &PeerId::random());

        //emit gossip
        gs.emit_gossip();

        //check that exactly config.gossip_lazy many gossip messages were sent.
        let msg_id = (gs.config.message_id_fn)(&message);
        assert_eq!(
            count_control_msgs(&gs, |peer, action| match action {
                GossipsubControlAction::IHave {
                    topic_hash,
                    message_ids,
                } => topic_hash == &topic_hashes[0] && message_ids.iter().any(|id| id == &msg_id),
                _ => false,
            }),
            config.gossip_lazy
        );
    }

    #[test]
    fn test_gossip_to_at_most_gossip_factor_peers() {
        let config = GossipsubConfig::default();

        //add a lot of peers
        let m = config.mesh_n_low + config.gossip_lazy * (2.0 / config.gossip_factor) as usize;
        let (mut gs, _, topic_hashes) = build_and_inject_nodes(m, vec!["topic".into()], true);

        //receive message
        let message = GossipsubMessage {
            source: Some(PeerId::random()),
            data: vec![],
            sequence_number: Some(0),
            topics: vec![topic_hashes[0].clone()],
            signature: None,
            key: None,
            validated: true,
        };
        gs.handle_received_message(message.clone(), &PeerId::random());

        //emit gossip
        gs.emit_gossip();

        //check that exactly config.gossip_lazy many gossip messages were sent.
        let msg_id = (gs.config.message_id_fn)(&message);
        assert_eq!(
            count_control_msgs(&gs, |peer, action| match action {
                GossipsubControlAction::IHave {
                    topic_hash,
                    message_ids,
                } => topic_hash == &topic_hashes[0] && message_ids.iter().any(|id| id == &msg_id),
                _ => false,
            }),
            ((m - config.mesh_n_low) as f64 * config.gossip_factor) as usize
        );
    }
}
