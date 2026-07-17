use core::num;
use std::{rc::Rc, sync::{Arc, RwLock, atomic::{AtomicBool, AtomicUsize, Ordering}}};

use tokio::{io::{self, AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, sync::Mutex};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use futures::{SinkExt, StreamExt};

use crate::protocol::infra_peer::ConnectionPacket;

pub static BOOTNODE_ADDRESS: &str = "127.0.0.1:1100";
static LEADER_ADDRESS: &str = "127.0.0.1:8070";

static UPDATED_LIST: AtomicBool = AtomicBool::new(false);

type AddressList = Rc<Mutex<Vec<Vec<u8>>>>;

pub async fn start_bootnode() {
	let listener = TcpListener::bind(BOOTNODE_ADDRESS).await
		.expect("Bootnode could not start");

	/* List/registry of all connected peers in the network */
	let address_list: AddressList  = Rc::new(Mutex::new(Vec::with_capacity(128)));
	let update_task_list = address_list.clone();

	let socket_codec = LengthDelimitedCodec::builder()
		.length_field_length(2).little_endian().new_codec();

	let update_codec = socket_codec.clone();

	// Task 1 - listen for changes to registry from leader node
	let mut update_listener_task = tokio::task::spawn_local(async move {
		let leader_socket = TcpStream::connect(LEADER_ADDRESS).await
			.expect("Failed to connect to leader socket");

		let mut socket_framed = 
			Framed::new(leader_socket, update_codec);

		let mut serialize_pool = BytesMut::with_capacity(1024);

		while let Some(Ok(mut addr_list)) = socket_framed.next().await {
			let mut write_lock = update_task_list.lock().await;

			let num_addresses = addr_list.len() / 14;

			if num_addresses > write_lock.capacity() {
				write_lock.reserve(num_addresses); }

			// while there are still address bytes in the network buffer
			// copy the entire value from the network into the list buffer
			while addr_list.len() >= 14 {
				// split off the address and create a packet from it
				let packet = ConnectionPacket {
					node_type: Bytes::from_static(b"client"),
					address: addr_list.split_to(14).freeze() };

				// serialize the packet
				bincode::serialize_into((&mut serialize_pool).writer(), &packet).unwrap();

				// extend the list by splitting off the newly serialized packet
				write_lock.extend_from_slice(&[serialize_pool.split().to_vec()]);

				// updated list - toggle updated list flag
				UPDATED_LIST.store(true, Ordering::Release);
			}
		}
	});

	let task_codec = socket_codec.clone();
	let join_task_list = address_list.clone();

	// Task 2 - listen to connections from new tasks
	let mut join_task = tokio::task::spawn_local(async move {
		let mut payload = BytesMut::with_capacity(1024);

		while let Ok((socket, addr)) = listener.accept().await {
			let list = &mut *join_task_list.lock().await;
			let codec = task_codec.clone();

			let mut socket_framed = 
				Framed::new(socket, codec);

			// construct the connection packet to send to the peer node
			let leader_packet = ConnectionPacket { 
				node_type: Bytes::from_static(b"leader"), 
				address: Bytes::from_static(LEADER_ADDRESS.as_bytes()) };

			// serialize and send leader connection packet
			match bincode::serialize(&leader_packet) {
				Ok(packet_buffer) => {
					let _ = socket_framed.send(Bytes::from(packet_buffer)).await;
				}, Err(_) => { eprintln!("failure"); }
			}

			// if the registry has not been updated, send it
			if UPDATED_LIST.load(Ordering::Acquire) {
				// create buffer heap space for serialization
				let len: usize = list.iter().map(|v| v.len()).sum();
				payload.clear(); payload.reserve(len);

				// loop - loop through all addresses and append them to payload
				for value in list {
					payload.extend_from_slice(&value); }
			}
			
			// send the full address list to the client
			let _ = socket_framed.send(payload.clone().freeze()).await;
		}
	});

	let _ = tokio::join!(update_listener_task, join_task);

	println!("bootnode at the end of select...");
}