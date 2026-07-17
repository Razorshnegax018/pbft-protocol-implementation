use std::collections::HashMap;
use std::io::ErrorKind;
use std::sync::Arc;

use rand::Rng;
use tokio::sync::mpsc;
use tokio::{io, net::{TcpListener, TcpStream}};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use futures::{StreamExt, SinkExt};

use bytes::{Bytes, BytesMut, BufMut};

use serde::{Serialize, Deserialize};

use crate::protocol::infra_main::{ClientTransaction, Transaction};

use rand::rngs::OsRng;
use ed25519_dalek::SigningKey;
use ed25519_dalek::Signature;

type PeerRegistry = Arc<Vec<TcpStream>>;
type LeaderSocket = Framed<TcpStream, LengthDelimitedCodec>;

static BOOTNODE_ADDRESS: &str = "127.0.0.1:1100";
static LEADER_ADDRESS: &str = "127.0.0.1:8070";

#[derive(Serialize, Deserialize)]
pub struct ConnectionPacket {
    pub node_type: Bytes, pub address: Bytes
}

type SocketFramed = Framed<TcpStream, LengthDelimitedCodec>;

fn make_framed(socket: TcpStream) -> SocketFramed {
    let socket_codec = LengthDelimitedCodec::builder()
        .length_field_length(2).little_endian().new_codec();

    Framed::new(socket, socket_codec)
}

fn deserialize_connection_packet(buffer: &[u8]) -> io::Result<ConnectionPacket> {
    match bincode::deserialize::<ConnectionPacket>(buffer) {
        Ok(packet) => Ok(packet),
        Err(_) => {
            eprintln!("Bootnode sent bad packet");
            Err(io::Error::new(ErrorKind::InvalidData, ""))
        }
    }
}

async fn start_peer_node() {
    let mut registry: Vec<TcpStream> = Vec::new();
    let mut _leader_socket: Option<TcpStream> = None;

    if let Ok(socket) = TcpStream::connect(BOOTNODE_ADDRESS).await {
        let mut socket_framed = make_framed(socket);

        // if deserialization of the connection packet from the bootnode fails,
        // log the error and skip the packet
        while let Some(Ok(value)) = socket_framed.next().await {
            let Ok(packet) = deserialize_connection_packet(&value) 
                else { continue; };

            // attempt connection to peer
            let address = String::from_utf8_lossy(&packet.address[..]);

            match TcpStream::connect(&*address).await {
                Ok(stream) => { if packet.node_type == "leader" 
                    { _leader_socket = Some(stream); } else { registry.push(stream) }},
                Err(_) => { eprintln!("Unable to connect to peer w/ addr: {}", &*address); }
            }
        }
    } 

    if _leader_socket.is_none() { panic!("No leader node found") }
    let leader_socket = 
        make_framed(_leader_socket.unwrap());

    start_server(Arc::new(registry), leader_socket).await;
}

async fn start_server(registry: PeerRegistry, leader_socket: LeaderSocket) {
    // loop to try tcp connection again if port is taken up
    loop { let mut rng = rand::thread_rng();
        
        // create a random port number to bind to 
        let port = rng.gen_range(1024..9999);

        // if that port doesn't work, then skip down to the end and loop again to make a new port
        if let Ok(listener) = TcpListener::bind(format!("127.0.0.1:{}", port)).await {
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

            let _socket = TcpStream::connect(LEADER_ADDRESS).await
                .expect("Could not connect to leader node");

            let leader_socket 
                = make_framed(_socket);

            // TODO: spawn the consensus engine as a tokio task

            while let Ok((socket, _)) = listener.accept().await {
                let registry_clone = registry.clone();

                // TODO - get rid of the rc refcell and implement the channel actor model
                tokio::task::spawn(async move { let _ = handle_request(socket, registry_clone).await; });
            }
        } else {} }
}

async fn handle_request(socket: TcpStream, registry: PeerRegistry) -> io::Result<()> {
    let socket_codec = LengthDelimitedCodec::builder()
        .length_field_length(2).little_endian().new_codec();

    let mut socket_framed = Framed::new(socket, socket_codec);

    // Step 0A - create a private-public keypair
    let mut csprng = OsRng;
    let private_key: SigningKey = SigningKey::generate(&mut csprng);


    // handle requests from clients to the protocol only at this stage
    while let Some(Ok(value)) = socket_framed.next().await {
        match bincode::deserialize::<ClientTransaction>(&value[..]) {
            Ok((transaction)) => {
                // Step 0: send the client mutation request to the leader

                // leader_framed.send().await;
                // request_client_mutation(transaction, &mut leader_socket);
            },
            Err(_) => { eprintln!("Deserialization failed"); }
        }
    }

    Ok(())
}

async fn peer_consensus_engine(tx: ClientTransaction, registry: PeerRegistry) {
    // connect to 
}

async fn request_client_mutation(transaction: ClientTransaction, leader_socket: &mut LeaderSocket) {

}