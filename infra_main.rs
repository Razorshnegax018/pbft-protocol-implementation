use std::{collections::HashMap, sync::{Arc, atomic::{AtomicU32, Ordering::SeqCst}}, time::Duration};

use tokio::sync::{Mutex, mpsc};

use sha2::{Sha256, Digest};

use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::{io::{self}, net::{TcpListener, TcpStream}};
use tokio_util::codec::{Encoder, Framed, LengthDelimitedCodec};

use futures::{StreamExt, SinkExt};

use bytes::{BufMut, Bytes, BytesMut};

use serde::{Serialize, Deserialize};

use dashmap::DashMap;

/** Type for storage - stores username/filename pairs as key, and then filename content */
pub type Storage = Arc<DashMap<(Bytes, Bytes), Bytes>>;

/** type for peer server list - stores server id as key, then the connection stream */
pub type ServerList = Arc<DashMap<Bytes, TcpStream>>;

pub type PeerRegistry = Arc<Mutex<Vec<OwnedWriteHalf>>>;

static ADDRESS: &str = "127.0.0.1:7070";

static SEQUENCE_COUNTER: AtomicU32 = AtomicU32::new(10);
static LEADER_VIEW_NUMBER: AtomicU32 = AtomicU32::new(5);

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

async fn start_server() {
    let listener = TcpListener::bind(ADDRESS).await.unwrap();
    let registry: PeerRegistry = Arc::new(Mutex::new(Vec::with_capacity(10)));
    println!("Server started at {}", ADDRESS);

    // create the manager sender and reciever queue ends
    let (sender, mut tx_reciever) = mpsc::channel::<Transaction>(32);

    // Start the manager task that controls state and the transaction log
    let _transaction_manager = tokio::task::spawn(async move {
        let mut blockchain_state: HashMap<Bytes, Bytes> = HashMap::new();
        let mut transaction_log: Vec<Transaction> = Vec::with_capacity(1024);

        while let Some(transaction) = tx_reciever.recv().await {
            blockchain_state.insert(transaction.key.clone(), transaction.value.clone());
            transaction_log.push(transaction);
        }
    });

    while let Ok((socket, _)) = listener.accept().await {
        let (peer_read_socket, registry_write_socket) = socket.into_split();
        println!("Peer node connected to primary");

        registry.lock().await.push(registry_write_socket);
        let peer_registry = registry.clone();
        let commit_sender = sender.clone();
        tokio::task::spawn(async move { 
            let _ = handle_request(peer_read_socket, peer_registry, commit_sender).await; });
        // TODO - deal with above error from handle request
    }
}

/* the only network request thing the leader node can accept
    are state mutation requests from other clients.
 */
async fn handle_request(socket: OwnedReadHalf, registry: PeerRegistry, 
    commit_sender: mpsc::Sender<Transaction>) -> io::Result<()> {
    let socket_codec = LengthDelimitedCodec::builder()
        .length_field_length(2).big_endian().new_codec();

    let mut socket_framed = Framed::new(socket, socket_codec);

    // framed next for the socket codec - framing entire messages
    if let Some(Ok(read_buffer)) = socket_framed.next().await {
        // we declare the mpsc channel here because every mutation,
        // not just standard peer connection, needs a specific reciever
        let (tx, rx) = mpsc::channel::<Bytes>(32);
        match bincode::deserialize::<Transaction>(&read_buffer) {
            Ok(value) => {
                // TODO: placeholder verification for cryptographic hashing
                // This is a poor placeholder for actual peer verification
                // But you understand the intent
                if value.source != Bytes::from_static(b"PEER") {
                    return Err(io::Error::new(io::ErrorKind::PermissionDenied, 
                    "Must be a peer node to talk to primary node"))
                } consensus_engine(value, registry, 
                    rx, commit_sender).await;

                // create a peer-specific sender for step 2
                let peer_tx = tx.clone();

                /* jump to step 2: spawn reader task to receive  */
                let _reader_task = tokio::task::spawn(async move {
                    // wait for PREPARE message from peer 
                    if let Some(Ok(prepare_vote)) = socket_framed.next().await {
                        // send this peer's prepare_vote to the primary
                        if let Err(_) = peer_tx.send(prepare_vote.freeze()).await 
                            { eprintln!("Primary node reciever hung up"); return; }
                    }
                });
            }, Err(_) => { eprintln!("Deserialization failed"); 
            return Err(io::Error::new(io::ErrorKind::InvalidData, "failed")) }
        }
    }
    Ok(())
}

async fn consensus_engine(mut transaction: Transaction, registry: PeerRegistry, 
    mut vote_reciever: mpsc::Receiver<Bytes>, commit_sender: mpsc::Sender<Transaction>) {
    // STEP 1: create monotonic sequence number and view number
    // broadcast the proposed transaction to each peer in the network registry
    let mut payload: BytesMut = BytesMut::new();

    let mut codec = LengthDelimitedCodec::builder()
        .length_field_length(2).big_endian().new_codec();
    
    // assign and overwrite new sequence and view numbers
    transaction.seq_counter = SEQUENCE_COUNTER.load(SeqCst);
    transaction.view_number = LEADER_VIEW_NUMBER.load(SeqCst);

    // Encode the transaction and prepare to send it
    let tx_bytes = bincode::serialize(&transaction).unwrap();
    let _ = codec.encode(Bytes::from(tx_bytes), &mut payload);
    
    // loop through registry and send proposal to all peer nodes
    for socket in &mut *registry.lock().await {
        let mut socket_frame = Framed::new(socket, codec.clone());
        // silence the error - a nonresponsive peer means assume faulty
        let _ = socket_frame.send(payload.clone().freeze()).await;
    }

    // STEP 2: hash the key and value to validate the proposal, and
    // wait for a quorum (2f + 1) of PREPARE votes from other peers
    let mut hasher = Sha256::new();
    let mut hasher_buffer = BytesMut::new();
    let mut quorum_counter = 0;

    hasher_buffer.put_u64(transaction.client_key); hasher_buffer.put(transaction.key.clone());
    hasher_buffer.put(transaction.value.clone()); hasher.update(hasher_buffer);

    let tx_hash: [u8; 32] = hasher.finalize().into();

    // if the hashes match, add primary client's vote to the counter
    if tx_hash == transaction.tx_hash { quorum_counter += 1; }
    
    tokio::select! {
        _ = wait_for_quorum(&mut vote_reciever, &mut quorum_counter, b"PREPARE") => { println!("Quorum has been reached"); }

        _ = tokio::time::sleep(Duration::from_secs(5)) => 
            { println!("Time limit for quorum exceeded, 
                prepare verification failed"); return; }
    }

    // clean up the counter and the voter queue in preparation for recieving the commmit votes
    quorum_counter = 0; while let Ok(_) = vote_reciever.try_recv() { /* clear */ }

    // STEP 3: Broadcast commit message and wait again for commit quorum
    for socket in &mut *registry.lock().await {
        let mut socket_frame = Framed::new(socket, codec.clone());
        let _ = socket_frame.send(Bytes::from_static(b"COMMIT")).await;
    }

    tokio::select! {
        _ = wait_for_quorum(&mut vote_reciever, &mut quorum_counter, b"COMMIT") => { println!("Quorum has been reached"); }

        _ = tokio::time::sleep(Duration::from_secs(5)) => 
            { println!("Time limit for quorum exceeded, 
                commit verification failed"); return; }
    }

    // consensus reached, transaction verified - commit!
    if let Err(_) = commit_sender.send(transaction).await {
        eprintln!("Could not commit state transition"); }

    // Update the sequence counter for the next transaction
    // (for peer verification of order and leader identity)
    SEQUENCE_COUNTER.fetch_add(1, SeqCst);

}

async fn wait_for_quorum(vote_reciever: &mut mpsc::Receiver<Bytes>, 
        quorum_counter: &mut i32, stage: &'static [u8]) {
    while let Some(vote) = vote_reciever.recv().await {
        if vote == Bytes::from_static(stage) { 
            *quorum_counter += 1; 
        } if *quorum_counter >= 3 { break; }
    }
}

pub async fn _main() {
    tokio::task::spawn(async move { start_server().await; });

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // start_client().await;
}