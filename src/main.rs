use std::str::FromStr;
use std::time::Duration;
use std::{io, iter};

use async_trait::async_trait;
use clap::Parser;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, StreamExt};
use libp2p::identity::Keypair;
use libp2p::swarm::dial_opts::DialOpts;
use libp2p::swarm::SwarmEvent;
use libp2p::{noise, request_response, yamux, Multiaddr, SwarmBuilder};

/// An executable that sends or receives a lot of bytes.
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Address this node listens on for incoming connections.
    #[arg(short, long)]
    listen_address: String,

    /// Address this node attempts to dial to.
    #[arg(short, long)]
    dial_address: Option<String>,

    /// Amount of 1KB messages to write or read.
    #[arg(short, long, default_value_t = 1)]
    message_size_in_kilobyte: u64,

    /// If active, we're the sending side. If not, we're the receiving side.
    #[arg(short, long)]
    send_request: bool,

    /// Amount of time to wait on idle connection.
    #[arg(short = 't', long, default_value_t = 100)]
    idle_connection_timeout_millis: u64,
}

#[derive(Clone)]
pub struct Codec {
    message_size_in_kilobyte: u64,
}

#[async_trait]
impl request_response::Codec for Codec {
    type Protocol = String;
    type Request = ();
    type Response = ();

    async fn read_request<T>(&mut self, _: &Self::Protocol, _: &mut T) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        Ok(())
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut buffer = [0u8; 1024];
        for _ in 0..self.message_size_in_kilobyte {
            io.read_exact(&mut buffer).await?;
        }
        Ok(())
    }

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        _: &mut T,
        _: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        _: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let buffer = [1u8; 1024];
        for _ in 0..self.message_size_in_kilobyte {
            io.write_all(&buffer).await?;
        }
        Ok(())
    }
}
#[tokio::main]
async fn main() {
    let args = Args::parse();

    let listen_address = Multiaddr::from_str(&args.listen_address)
        .expect(&format!("Unable to parse address {}", args.listen_address));

    let key_pair = Keypair::generate_ed25519();
    let mut swarm = SwarmBuilder::with_existing_identity(key_pair)
        .with_tokio()
        .with_tcp(Default::default(), noise::Config::new, yamux::Config::default)
        .expect("Error while building the swarm")
        .with_behaviour(|_| {
            request_response::Behaviour::with_codec(
                Codec { message_size_in_kilobyte: args.message_size_in_kilobyte },
                iter::once(("/protocol".to_owned(), request_response::ProtocolSupport::Full)),
                Default::default(),
            )
        })
        .expect("Error while building the swarm")
        .with_swarm_config(|cfg| {
            cfg.with_idle_connection_timeout(Duration::from_millis(
                args.idle_connection_timeout_millis,
            ))
        })
        .build();
    swarm
        .listen_on(listen_address)
        .expect(&format!("Error while binding to {}", args.listen_address));

    if let Some(dial_address_str) = args.dial_address.as_ref() {
        let dial_address = Multiaddr::from_str(dial_address_str)
            .expect(&format!("Unable to parse address {}", dial_address_str));
        swarm
            .dial(DialOpts::unknown_peer_id().address(dial_address).build())
            .expect(&format!("Error while dialing {}", dial_address_str));
    }

    let mut got_response = false;

    while let Some(event) = swarm.next().await {
        match event {
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                if args.send_request {
                    swarm.behaviour_mut().send_request(&peer_id, ());
                }
            }
            SwarmEvent::Behaviour(request_response::Event::Message {
                message: request_response::Message::Request { channel, .. },
                ..
            }) => {
                swarm.behaviour_mut().send_response(channel, ()).unwrap();
            }
            SwarmEvent::Behaviour(request_response::Event::Message {
                message: request_response::Message::Response { .. },
                ..
            }) => {
                println!(
                    "The bug did not occur, we got the response. Try to run with a larger message \
                     or smaller timeout to make the bug occur"
                );
                got_response = true;
            }
            SwarmEvent::ConnectionClosed { .. } => {
                if !got_response && args.send_request {
                    println!(
                        "The bug occurred! The connection was closed before we got the response"
                    );
                }
            }
            _ => {}
        }
    }
}
