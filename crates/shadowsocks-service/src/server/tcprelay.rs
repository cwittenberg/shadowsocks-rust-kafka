
use lazy_static::lazy_static;


use std::{
    future::Future,
    io::{self, ErrorKind},
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
    collections::HashMap,
};

use log::{debug, error, info, trace, warn};
use shadowsocks::{
    crypto::CipherKind,
    net::{AcceptOpts, TcpStream as OutboundTcpStream},
    relay::tcprelay::{utils::copy_encrypted_bidirectional, ProxyServerStream},
    ProxyListener,
    ServerConfig
};
use std::fs;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream as TokioTcpStream,
    sync::Mutex,
    time,
};

use crate::net::{utils::ignore_until_end, MonProxyStream};

use super::context::ServiceContext;

use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::config::ClientConfig;
use get_if_addrs::{get_if_addrs, IfAddr};


use serde::{Serialize, Deserialize};


use std::collections::HashSet;



#[derive(Deserialize, Debug)]
struct CloudConfig {
    bootstrap_servers: String,
    security_protocol: String,
    sasl_mechanisms: String,
    sasl_username: String,
    sasl_password: String,
    session_timeout_ms: String,
    iface: String,
}

lazy_static! {
    // Globally accessible CloudConfig
    static ref CONFIG: CloudConfig = read_cloud_config();
}

fn read_cloud_config() -> CloudConfig {
    let config_str = fs::read_to_string("cloud.json")
        .expect("Failed to read cloud.json");
    serde_json::from_str(&config_str)
        .expect("Failed to parse cloud.json")
}


pub struct TcpServer {
    context: Arc<ServiceContext>,
    svr_cfg: ServerConfig,
    listener: ProxyListener,
    active_connections: Arc<Mutex<HashMap<u16, Instant>>>,
    kafka_producer: FutureProducer,
}

fn get_ip(interface: &str) -> Option<String> {
    if let Ok(addresses) = get_if_addrs() {
        for iface in addresses {
            if iface.name == interface {
                if let IfAddr::V6(v6) = iface.addr {
                    return Some(v6.ip.to_string());
                }
                if let IfAddr::V4(v4) = iface.addr {
                    return Some(v4.ip.to_string());
                }
            }
        }
    }
    None
}

fn create_kafka_producer() -> FutureProducer {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &CONFIG.bootstrap_servers)
        .set("security.protocol", &CONFIG.security_protocol)
        .set("sasl.mechanisms", &CONFIG.sasl_mechanisms)
        .set("sasl.username", &CONFIG.sasl_username)
        .set("sasl.password", &CONFIG.sasl_password)
        .set("session.timeout.ms", &CONFIG.session_timeout_ms)
        .create()
        .expect("Kafka producer creation failed");

    producer
}

impl TcpServer {    

    pub(crate) async fn new(
        context: Arc<ServiceContext>,
        svr_cfg: ServerConfig,
        accept_opts: AcceptOpts
    ) -> io::Result<TcpServer> {
        let listener = ProxyListener::bind_with_opts(context.context(), &svr_cfg, accept_opts).await?;

        let producer = create_kafka_producer();

        Ok(TcpServer {
            context,
            svr_cfg,
            listener,
            active_connections: Arc::new(Mutex::new(HashMap::new())),
            kafka_producer: producer,
        })
    }

    /// Server's configuration
    pub fn server_config(&self) -> &ServerConfig {
        &self.svr_cfg
    }

    /// Server's listen address
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub async fn run(self) -> io::Result<()> {
        let server_addr = self.listener.local_addr().expect("listener.local_addr");
        let server_port = server_addr.port();

        let iface = &CONFIG.iface;
        let ip=get_ip(iface).expect("eth0 interface is expected to either have an assigned ipv6 or ipv4 address"); //"8.8.8.8";
        let ip_clone = ip.clone();

        info!(
            "shadowsocks tcp server listening on port {}, inbound address {}",
            server_port,
            self.svr_cfg.addr()
        );

        let active_connections = self.active_connections.clone();
        let active_connections_for_spawn = active_connections.clone();

        tokio::spawn({
            let active_connections = active_connections_for_spawn.clone();
            let kafka_producer = self.kafka_producer.clone();
            async move {
                let mut interval = time::interval(Duration::from_secs(60));
                loop {
                    interval.tick().await;
                    let mut dropped_connections = Vec::new();
                    {
                        let mut connections = active_connections.lock().await;
                        connections.retain(|&port, last_active| {
                            if last_active.elapsed() < Duration::from_secs(60) {
                                true
                            } else {
                                dropped_connections.push(port);
                                false
                            }
                        });
                    }

                    for port in dropped_connections {
                        // let message = format!("Dropped {}:{}", ip_clone, port);
                        let message = format!(r#"{{"event": "dropped", "ip": "{}", "port": {}}}"#, ip_clone, port);

                        info!("{}", &message);
                    
                        // Clone kafka_producer for each iteration of the loop
                        let kafka_producer = kafka_producer.clone();
                    
                        // Using Tokio's spawn to not wait for the future to complete.
                        tokio::spawn(async move {
                            match kafka_producer.send(
                                FutureRecord::<(), String>::to("ports").payload(&message),
                                Duration::from_secs(0),
                            ).await {
                                Ok(_) => info!("Message sent successfully to Kafka"),
                                Err(e) => {
                                    println!("Failed to send message to Kafka: {:?}", e);
                                },
                            }
                        });
                    }
                }
            }
        });


        loop {
            let flow_stat = self.context.flow_stat();

            let (local_stream, peer_addr) = match self
                .listener
                .accept_map(|s| MonProxyStream::from_stream(s, flow_stat))
                .await
            {
                Ok(s) => s,
                Err(err) => {
                    error!("tcp server accept failed with error: {}", err);
                    time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };

            {
                let mut connections = active_connections.lock().await;
                if connections.insert(server_port, Instant::now()).is_none() {
                    // let message = format!("Created {}:{}", ip, server_port);
                    let message = format!(r#"{{"event": "consumed", "ip": "{}", "port": {}}}"#, ip, server_port);


                    info!("{}", &message);
            
                    // Clone the necessary data to be moved into the async block
                    let kafka_producer = self.kafka_producer.clone();
            
                    // Using Tokio's spawn to not wait for the future to complete.
                    tokio::spawn(async move {
                        match kafka_producer.send(
                            FutureRecord::<(), String>::to("ports").payload(&message),
                            Duration::from_secs(0),
                        ).await {
                            Ok(_) => info!("Message sent successfully to Kafka"),
                            Err(e) => {
                                // Directly printing the error for testing purposes
                                println!("Failed to send message to Kafka: {:?}", e);
                            },
                        }
                    });
                }
            }
            
            

            // let active_connections_clone = active_connections.clone();

            let client = TcpServerClient {
                context: self.context.clone(),
                method: self.svr_cfg.method(),
                peer_addr,
                stream: local_stream,
                timeout: self.svr_cfg.timeout(),
            };

            tokio::spawn(async move {
                if let Err(err) = client.serve().await {
                    debug!("tcp server stream for {} aborted with error: {}", peer_addr, err);
                    // info!("tcp client disconnected from {}", server_addr);
                } else {
                    // If there's no error, log a normal disconnection message
                    // info!("tcp client disconnected from {}", server_addr);
                }

                // Remove connection from active list
                // active_connections_clone.lock().await.remove(&peer_addr);
            });
        }
    }
}

#[inline]
async fn timeout_fut<F, R>(duration: Option<Duration>, f: F) -> io::Result<R>
where
    F: Future<Output = io::Result<R>>,
{
    match duration {
        None => f.await,
        Some(d) => match time::timeout(d, f).await {
            Ok(o) => o,
            Err(..) => Err(ErrorKind::TimedOut.into()),
        },
    }
}

struct TcpServerClient {
    context: Arc<ServiceContext>,
    method: CipherKind,
    peer_addr: SocketAddr,
    stream: ProxyServerStream<MonProxyStream<TokioTcpStream>>,
    timeout: Option<Duration>,
}

impl TcpServerClient {
    async fn serve(mut self) -> io::Result<()> {
        // let target_addr = match Address::read_from(&mut self.stream).await {
        let target_addr = match timeout_fut(self.timeout, self.stream.handshake()).await {
            Ok(a) => a,
            // Err(Socks5Error::IoError(ref err)) if err.kind() == ErrorKind::UnexpectedEof => {
            //     debug!(
            //         "handshake failed, received EOF before a complete target Address, peer: {}",
            //         self.peer_addr
            //     );
            //     return Ok(());
            // }
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => {
                debug!(
                    "tcp handshake failed, received EOF before a complete target Address, peer: {}",
                    self.peer_addr
                );
                return Ok(());
            }
            Err(err) if err.kind() == ErrorKind::TimedOut => {
                debug!(
                    "tcp handshake failed, timeout before a complete target Address, peer: {}",
                    self.peer_addr
                );
                return Ok(());
            }
            Err(err) => {
                // https://github.com/shadowsocks/shadowsocks-rust/issues/292
                //
                // Keep connection open. Except AEAD-2022
                warn!("tcp handshake failed. peer: {}, {}", self.peer_addr, err);

                #[cfg(feature = "aead-cipher-2022")]
                if self.method.is_aead_2022() {
                    // Set SO_LINGER(0) for misbehave clients, which will eventually receive RST. (ECONNRESET)
                    // This will also prevent the socket entering TIME_WAIT state.

                    let stream = self.stream.into_inner().into_inner();
                    let _ = stream.set_linger(Some(Duration::ZERO));

                    return Ok(());
                }

                debug!("tcp silent-drop peer: {}", self.peer_addr);

                // Unwrap and get the plain stream.
                // Otherwise it will keep reporting decryption error before reaching EOF.
                //
                // Note: This will drop all data in the decryption buffer, which is no going back.
                let mut stream = self.stream.into_inner();

                let res = ignore_until_end(&mut stream).await;

                trace!(
                    "tcp silent-drop peer: {} is now closing with result {:?}",
                    self.peer_addr,
                    res
                );

                return Ok(());
            }
        };

        trace!(
            "accepted tcp client connection {}, establishing tunnel to {}",
            self.peer_addr,
            target_addr
        );

        if self.context.check_outbound_blocked(&target_addr).await {
            error!(
                "tcp client {} outbound {} blocked by ACL rules",
                self.peer_addr, target_addr
            );
            return Ok(());
        }

        let mut remote_stream = match timeout_fut(
            self.timeout,
            OutboundTcpStream::connect_remote_with_opts(
                self.context.context_ref(),
                &target_addr,
                self.context.connect_opts_ref(),
            ),
        )
        .await
        {
            Ok(s) => s,
            Err(err) => {
                error!(
                    "tcp tunnel {} -> {} connect failed, error: {}",
                    self.peer_addr, target_addr, err
                );
                return Err(err);
            }
        };

        // https://github.com/shadowsocks/shadowsocks-rust/issues/232
        //
        // Protocols like FTP, clients will wait for servers to send Welcome Message without sending anything.
        //
        // Wait at most 500ms, and then sends handshake packet to remote servers.
        if self.context.connect_opts_ref().tcp.fastopen {
            let mut buffer = [0u8; 8192];
            match time::timeout(Duration::from_millis(500), self.stream.read(&mut buffer)).await {
                Ok(Ok(0)) => {
                    // EOF. Just terminate right here.
                    return Ok(());
                }
                Ok(Ok(n)) => {
                    // Send the first packet.
                    timeout_fut(self.timeout, remote_stream.write_all(&buffer[..n])).await?;
                }
                Ok(Err(err)) => return Err(err),
                Err(..) => {
                    // Timeout. Send handshake to server.
                    timeout_fut(self.timeout, remote_stream.write(&[])).await?;

                    trace!(
                        "tcp tunnel {} -> {} sent TFO connect without data",
                        self.peer_addr,
                        target_addr
                    );
                }
            }
        }

        debug!(
            "established tcp tunnel {} <-> {} with {:?}",
            self.peer_addr,
            target_addr,
            self.context.connect_opts_ref()
        );

        match copy_encrypted_bidirectional(self.method, &mut self.stream, &mut remote_stream).await {
            Ok((rn, wn)) => {
                trace!(
                    "tcp tunnel {} <-> {} closed, L2R {} bytes, R2L {} bytes",
                    self.peer_addr,
                    target_addr,
                    rn,
                    wn
                );
            }
            Err(err) => {
                trace!(
                    "tcp tunnel {} <-> {} closed with error: {}",
                    self.peer_addr,
                    target_addr,
                    err
                );
            }
        }

        Ok(())
    }
}