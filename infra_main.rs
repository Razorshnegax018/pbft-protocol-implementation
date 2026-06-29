use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::{sync::{mpsc}, time::Instant};

use sha2::{Sha256, Digest};

use tokio::{io::{self}, net::{TcpListener, TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf}}};
use tokio_util::{codec::{Encoder, Framed, LengthDelimitedCodec}};

use futures::{SinkExt, StreamExt};

use bytes::{BufMut, Bytes, BytesMut};

use serde::{Serialize, Deserialize};

use dashmap::DashMap;

/** Type for storage - stores username/filename pairs as key, and then filename content */
pub type Storage = Arc<DashMap<(Bytes, Bytes), Bytes>>;

/** type for peer server list - stores server id as key, then the connection stream */
pub type ServerList = Arc<DashMap<Bytes, TcpStream>>;
type SocketFramed = Framed<OwnedWriteHalf, LengthDelimitedCodec>;

type PeerRegistry = Vec<(SocketFramed, std::net::SocketAddr)>;

static ADDRESS: &str = "127.0.0.1:8070";
static BOOTNODE_ADDRESS: &str = "127.0.0.1:1100";

/** Standard transaction struct that peers append to the block log */
#[derive(Serialize, Deserialize)]
pub struct Transaction {
    pub consensus_stage: String, pub tx_hash: [u8; 32], 
    pub seq_counter: u32, pub view_number: u32, pub client_key: u64,
    pub source: Bytes, pub key: Bytes, pub value: Bytes,
}

/** Data that the client sends to a peer when requesting a mutation */
#[derive(Serialize, Deserialize)]
pub struct ClientTransaction {
    pub key: Bytes, pub value: Bytes, pub client_key: Bytes
}

/** Struct that packages required tools for consensus 
 (view number, sequence counter, registry) into one struct
 that can all be unlocked and accessed with a single mutex unlock
*/
pub struct ConsensusToolsStruct {
    pub sequence_counter: u32, pub view_number: u32, pub registry: PeerRegistry
}

pub type ConsensusTools = ConsensusToolsStruct;

#[derive(Serialize, Deserialize)]
pub enum ActorRequest {
    ConsensusRequest { transaction: Bytes }, PeerVote { vote_type: Bytes }
}

pub struct RegistrationRequest { socket: OwnedWriteHalf, addr: std::net::SocketAddr }

pub async fn start_server(network_runtime: tokio::runtime::Handle) -> Result<(), io::Error> {
    let registry: PeerRegistry = Vec::with_capacity(10);
    let listener = TcpListener::bind(ADDRESS).await.unwrap();
    let tools = ConsensusToolsStruct { sequence_counter: 0, view_number: 0, registry };

    println!("Server started at {}", ADDRESS);

    // create the manager sender and receiver queue ends
    let (transaction_sender, mut transaction_receiver) = mpsc::channel::<Transaction>(32);

    /*
     * primary method of communicating with the hot loop
     * @returns peer_tx: used by the reader_task so they can peers can communicate with hot loop task
     * 
     */
    let (peer_tx, peer_receiver) = mpsc::channel::<ActorRequest>(512);

    /* channel sender and receiver to add a new peer to the registry */
    let (registration_tx, registration_rx) = 
        local_channel::mpsc::channel::<RegistrationRequest>();

    // Start the manager task that controls state and the transaction log
    let _transaction_manager = tokio::task::spawn_local(async move {
        let mut blockchain_state: HashMap<Bytes, Bytes> = HashMap::new();
        let mut transaction_log: Vec<Transaction> = Vec::with_capacity(1024);

        while let Some(transaction) = transaction_receiver.recv().await {
            blockchain_state.insert(transaction.key.clone(), transaction.value.clone());
            transaction_log.push(transaction);
        }
    });

    let commit_sender = transaction_sender.clone();

    let reader_runtime = network_runtime.clone();

    // Start the consensus actor task
    let _consensus_task = tokio::task::spawn_local(
        consensus_actor(commit_sender, tools, peer_receiver, registration_rx));

    // For each client that joins:
    while let Ok((socket, addr)) = listener.accept().await {
        println!("Peer node {addr} connected to primary");

        let (read_socket, write_socket) = socket.into_split();

        // pass the read half and the request sender to a new reader task
        let _peer_tx = peer_tx.clone();
        reader_runtime.spawn(reader_task(read_socket, _peer_tx)); 

        // send a registration request to the hot loop receiver to register the write half
        let request = RegistrationRequest { socket: write_socket, addr };
        let _ = registration_tx.send(request);
    }

    Ok(())
}

/// @function consensus actor: actor handler for the consensus engine hot loop
///  * @param commit_sender: Sender end of channel engine uses 
///    to send completed transactions back to state manager 
///  * @param tools: criical tools (seq_counter, view_number, peer_socket_registry)
///    required for consensus
///  * @param peer_receiver: entryway for peer sockets to communicate with consensus
async fn consensus_actor(
    mut commit_sender: mpsc::Sender<Transaction>, mut tools: ConsensusTools, 
    mut peer_receiver: mpsc::Receiver<ActorRequest>, 
    mut registration_rx: local_channel::mpsc::Receiver<RegistrationRequest>) -> io::Result<()> {
        
    let bootnode_socket = TcpStream::connect(BOOTNODE_ADDRESS).await
        .expect("Connection to bootnode failed");

    let socket_codec = LengthDelimitedCodec::builder()
        .length_field_length(2).little_endian().new_codec();

    let mut bootnode_framed = 
        Framed::new(bootnode_socket, socket_codec.clone());


    tokio::select! {
        biased;

        // wait for transaction request
        Some(request) = peer_receiver.recv() => {
            match request {
                // client/peer node request to commit a transaction
                ActorRequest::ConsensusRequest { transaction } => {
                    let tx = bincode::deserialize::<Transaction>(&transaction).unwrap();
        
                    consensus_engine(tx, &mut tools, 
                    &mut peer_receiver, &mut commit_sender).await;
                },
                _ => eprintln!("Invalid message from peer")
            }
        }

        Some(request) = registration_rx.recv() => {
            // onboarding task request to add a new peer to the registry
            let socket_framed = Framed::new(request.socket, socket_codec.clone());
                tools.registry.push((socket_framed, request.addr));

            // whenever there's a new peer, send the updated registry to the bootnode
            let addresses: Bytes = tools.registry.into_iter().flat_map(|(_socket, addr)| 
                addr.to_string().into_bytes()).collect();

            // send the entire list as a contiguous buffer, but then
            // used a fixed-len codec to take the addresses one by one on the bootnode side
            let _ = bootnode_framed.send(Bytes::from(addresses)).await;
        }
    }

    Ok(())
}

/// @function passed to the multithreaded runtime. 
/// Spanwed as a task-per-connection architecture to route each connection's 
/// network request to the consensus/hot loop actor
/// * @param read_socket: the read half of the connection's socket, 
/// to receive that connection's requests
/// * @param peer_tx: sender channel to send transaction requests to the consensus actor
async fn reader_task(read_socket: OwnedReadHalf, peer_tx: mpsc::Sender<ActorRequest>) -> io::Result<()> {
    // create the codec and frameed for the reader socket
    let socket_codec = LengthDelimitedCodec::builder()
        .length_field_length(2).little_endian().new_codec();

    let mut socket_framed = Framed::new(read_socket, socket_codec);

    // wait for messages from peer
    // framed next for the socket codec - framing entire messages
    while let Some(Ok(read_buffer)) = socket_framed.next().await {
        // Step 1 - deserailize client request (change for proper error handling)
        match bincode::deserialize::<ActorRequest>(&read_buffer) {
            // Step 2 - send either transaction payload or vote to the consensus actor
            // (all recievers expect payload type ActorRequest 
            // so just pass along the deserialized request)
            Ok(request) => { let _ = peer_tx.send(request).await; },
            Err(_) => { eprintln!("Deserialization failed"); 
                return Err(io::Error::new(io::ErrorKind::InvalidData, "failed")) }
            
        }
    }

    Ok(())
}

async fn consensus_engine(transaction: Transaction, tools: &mut ConsensusTools, 
    mut vote_reciever: &mut mpsc::Receiver<ActorRequest>, commit_sender: &mut mpsc::Sender<Transaction>) {
    let (view_number, seq_counter) = (tools.view_number, tools.sequence_counter);

    // STEP 1: broadcast the proposed transaction to each peer in the network registry
    let mut payload: BytesMut = BytesMut::new();

    let mut codec = LengthDelimitedCodec::builder()
        .length_field_length(2).little_endian().new_codec();

    // Encode the transaction and prepare to send it
    let tx_bytes = bincode::serialize(&transaction).unwrap();
    let _ = codec.encode(Bytes::from(tx_bytes), &mut payload);

    let sleep = tokio::time::sleep(Duration::from_millis(100));

    tokio::pin!(sleep);

    let deadline = Instant::now() + Duration::from_millis(100);
    sleep.as_mut().reset(deadline.into());
    
    // loop through registry and send proposal to all peer nodes
    for (socket_frame, _) in &mut tools.registry {
        // set a timout - we don't want to hang sending to nonresponsive peers
        tokio::select! {
            _ = socket_frame.send(payload.clone().freeze()) => {}
            _ = &mut sleep => { /* peer eviction */ }
        }
    }

    // STEP 2: hash the key and value to validate the proposal, and
    // wait for a quorum (2f + 1) of PREPARE votes from other peers
    let mut quorum_counter = 0; sleep.as_mut().reset(deadline.into());

    verify_transaction(&transaction, &mut quorum_counter, seq_counter, view_number);

    tokio::select! {
        _ = wait_for_quorum(vote_reciever, &mut quorum_counter, b"PREPARE") => { println!("Quorum has been reached"); }

        _ = &mut sleep => 
            { println!("Time limit for quorum exceeded, 
                prepare verification failed"); return; }
    }
    

    // clean up the counter and the voter queue in preparation for recieving the commmit votes
    quorum_counter = 0; while let Ok(_) = vote_reciever.try_recv() { /* clear */ }
    sleep.as_mut().reset(deadline.into());

    // STEP 3: Broadcast commit message and wait again for commit quorum
    for (socket_frame, _) in &mut tools.registry {
        // set a timout - we don't want to hang sending to nonresponsive peers
        tokio::select! {
            _ = socket_frame.send(Bytes::from_static(b"COMMIT")) => {}
            _ = &mut sleep => {}
        }
    }

    tokio::select! {
        _ = wait_for_quorum(&mut vote_reciever, &mut quorum_counter, b"COMMIT") => { println!("Quorum has been reached"); }

        _ = &mut sleep => 
            { println!("Time limit for quorum exceeded, 
                commit verification failed"); return; }
    }

    // consensus reached, transaction verified - commit!
    // if state transition fails (split brain) - fail fast and resync after restarting
    if let Err(_) = commit_sender.send(transaction).await {
        eprintln!("Could not commit state transition");
        panic!("Node {ADDRESS} crashed due to state transition failure"); 
    }

    // Update the sequence counter for the next transaction
    // (for peer verification of order and leader identity)
    tools.sequence_counter += 1;

    // update the leader view number
    if seq_counter >= 10 {
        tools.sequence_counter = 0; tools.view_number += 1;
    }

}

fn verify_transaction(transaction: &Transaction, quorum_counter: &mut i32, seq_counter: u32, view_number: u32) {
    let mut hasher = Sha256::new();
    let mut hasher_buffer = BytesMut::new();

    hasher_buffer.put_u64(transaction.client_key); hasher_buffer.put(transaction.key.clone());
    hasher_buffer.put(transaction.value.clone()); hasher.update(hasher_buffer);

    let tx_hash: [u8; 32] = hasher.finalize().into();

    // verify the sequence counter and view number
    let counter_valid = 
        seq_counter == transaction.seq_counter &&
        view_number == transaction.view_number;

    // if the hashes match, add primary client's vote to the counter
    if tx_hash == transaction.tx_hash && counter_valid == true { *quorum_counter += 1; }
}

async fn wait_for_quorum(vote_reciever: &mut mpsc::Receiver<ActorRequest>, 
        quorum_counter: &mut i32, stage: &'static [u8]) {

    while let Some(value) = vote_reciever.recv().await {
        match value {
            ActorRequest::PeerVote { vote_type } => {
                if vote_type == Bytes::from_static(stage) { 
                    *quorum_counter += 1; 
                } if *quorum_counter >= 3 { break; }
            }, _ => {}
        }
    }
}
