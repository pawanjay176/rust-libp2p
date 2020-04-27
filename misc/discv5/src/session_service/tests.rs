#![cfg(test)]
use super::*;
use crate::rpc::{Request, Response, RpcType};
use enr::EnrBuilder;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::timeout;

fn init() {
    let _ = env_logger::builder().is_test(true).try_init();
}

#[tokio::test]
// Tests the construction and sending of a simple message
async fn simple_session_message() {
    env_logger::init();

    let sender_port = 5000;
    let receiver_port = 5001;
    let ip: IpAddr = "127.0.0.1".parse().unwrap();

    let key1 = CombinedKey::generate_secp256k1();
    let key2 = CombinedKey::generate_secp256k1();

    let config = Discv5Config::default();

    let sender_enr = EnrBuilder::new("v4")
        .ip(ip)
        .udp(sender_port)
        .build(&key1)
        .unwrap();
    let receiver_enr = EnrBuilder::new("v4")
        .ip(ip)
        .udp(receiver_port)
        .build(&key2)
        .unwrap();

    let mut sender_service = SessionService::new(
        sender_enr.clone(),
        key1,
        sender_enr.udp_socket().unwrap(),
        config.clone(),
    )
    .await
    .unwrap();

    let mut receiver_service = SessionService::new(
        receiver_enr.clone(),
        key2,
        receiver_enr.udp_socket().unwrap(),
        config,
    )
    .await
    .unwrap();

    let send_message = ProtocolMessage {
        id: 1,
        body: RpcType::Request(Request::Ping { enr_seq: 1 }),
    };

    let receiver_send_message = send_message.clone();

    let _ = sender_service
        .send_request(&receiver_enr, send_message)
        .unwrap();

    let sender = async {
        loop {
            let fut = |cx: &mut task::Context<'_>| sender_service.poll(cx);
            let _ = future::poll_fn(fut).await;
        }
    };

    let receiver = async {
        loop {
            let fut = |cx: &mut task::Context<'_>| receiver_service.poll(cx);
            let message = future::poll_fn(fut).await;

            match message {
                SessionEvent::WhoAreYouRequest { src, auth_tag, .. } => {
                    let seq = sender_enr.seq();
                    let node_id = &sender_enr.node_id();
                    receiver_service.send_whoareyou(
                        src,
                        node_id,
                        seq,
                        Some(sender_enr.clone()),
                        auth_tag,
                    );
                }
                SessionEvent::Message { message, .. } => {
                    dbg!(&message);
                    assert_eq!(*message, receiver_send_message);
                    break;
                }
                _ => {}
            }
        }
    };

    let future = futures::future::select(Box::pin(sender), Box::pin(receiver));
    let test_result = Arc::new(Mutex::new(true));
    let thread_result = test_result.clone();
    if let Err(_) = timeout(Duration::from_millis(100), future).await {
        *thread_result.lock().unwrap() = false;
    }
    assert!(*test_result.lock().unwrap());
}

#[tokio::test]
// Tests sending multiple messages on an encrypted session
async fn multiple_messages() {
    init();
    let sender_port = 5002;
    let receiver_port = 5003;
    let ip: IpAddr = "127.0.0.1".parse().unwrap();
    let key1 = CombinedKey::generate_secp256k1();
    let key2 = CombinedKey::generate_secp256k1();

    let sender_enr = EnrBuilder::new("v4")
        .ip(ip)
        .udp(sender_port)
        .build(&key1)
        .unwrap();
    let receiver_enr = EnrBuilder::new("v4")
        .ip(ip)
        .udp(receiver_port)
        .build(&key2)
        .unwrap();

    let mut sender_service = SessionService::new(
        sender_enr.clone(),
        key1,
        sender_enr.udp_socket().unwrap(),
        Discv5Config::default(),
    )
    .await
    .unwrap();
    let mut receiver_service = SessionService::new(
        receiver_enr.clone(),
        key2,
        receiver_enr.udp_socket().unwrap(),
        Discv5Config::default(),
    )
    .await
    .unwrap();

    let send_message = ProtocolMessage {
        id: 1,
        body: RpcType::Request(Request::Ping { enr_seq: 1 }),
    };

    let pong_response = ProtocolMessage {
        id: 1,
        body: RpcType::Response(Response::Ping {
            enr_seq: 1,
            ip,
            port: sender_port,
        }),
    };

    let receiver_send_message = send_message.clone();

    let messages_to_send: usize = 5;

    // sender to send the first message then await for the session to be established
    let _ = sender_service.send_request(&receiver_enr, send_message.clone());

    let mut message_count: usize = 0;

    let sender = async {
        loop {
            let fut = |cx: &mut task::Context<'_>| match sender_service.poll(cx) {
                Poll::Ready(SessionEvent::Established(_)) => {
                    // now the session is established, send the rest of the messages
                    for _ in 0..messages_to_send - 1 {
                        let _ = sender_service.send_request(&receiver_enr, send_message.clone());
                    }
                    return Poll::Pending;
                }
                Poll::Ready(x) => return Poll::Ready(x),
                Poll::Pending => return Poll::Pending,
            };
            let _ = future::poll_fn(fut).await;
        }
    };

    let receiver = async {
        loop {
            // let message = match receiver_service.poll() {
            //     Async::Ready(message) => message,
            //     Async::NotReady => return Ok(Async::NotReady),
            // };
            let fut = |cx: &mut task::Context<'_>| receiver_service.poll(cx);
            let message = future::poll_fn(fut).await;

            match message {
                SessionEvent::WhoAreYouRequest { src, auth_tag, .. } => {
                    let seq = sender_enr.seq();
                    let node_id = &sender_enr.node_id();
                    receiver_service.send_whoareyou(
                        src,
                        node_id,
                        seq,
                        Some(sender_enr.clone()),
                        auth_tag,
                    );
                }
                SessionEvent::Message { message, .. } => {
                    assert_eq!(*message, receiver_send_message);
                    message_count += 1;
                    // required to send a pong response to establish the session
                    let _ = receiver_service.send_request(&sender_enr, pong_response.clone());
                    if message_count == messages_to_send {
                        break;
                    }
                }
                _ => {}
            }
        }
    };

    let future = futures::future::select(Box::pin(sender), Box::pin(receiver));
    let test_result = Arc::new(Mutex::new(true));
    let thread_result = test_result.clone();
    if let Err(_) = timeout(Duration::from_millis(100), future).await {
        *thread_result.lock().unwrap() = false;
    }
    assert!(*test_result.lock().unwrap());
}
