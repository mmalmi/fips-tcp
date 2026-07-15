use std::collections::VecDeque;

use fips_tcp::wire::{FIPS_VERSION, Flags, Segment};
use fips_tcp::{Config as FipsConfig, ConnectionId, Stack, State as FipsState};
use smoltcp::iface::{Config as InterfaceConfig, Interface, SocketHandle, SocketSet};
use smoltcp::phy::{ChecksumCapabilities, Device, DeviceCapabilities, Medium};
use smoltcp::socket::tcp::{Socket as TcpSocket, SocketBuffer, State as SmolState};
use smoltcp::time::Instant;
use smoltcp::wire::{
    HardwareAddress, IpAddress, IpCidr, IpProtocol, Ipv4Address, Ipv4Packet, Ipv4Repr,
    TcpOption as SmolTcpOption, TcpPacket,
};

const FIPS_PEER: &str = "smoltcp";
const FIPS_IP: Ipv4Address = Ipv4Address::new(192, 0, 2, 1);
const SMOL_IP: Ipv4Address = Ipv4Address::new(192, 0, 2, 2);
const SERVER_PORT: u16 = 39_017;
const CLIENT_PORT: u16 = 49_152;
const FIPS_OPTION: [u8; 4] = [254, 4, FIPS_VERSION, 0];

#[derive(Default)]
struct QueueDevice {
    rx: VecDeque<Vec<u8>>,
    tx: VecDeque<Vec<u8>>,
}

impl Device for QueueDevice {
    type RxToken<'a> = ReceiveToken;
    type TxToken<'a> = TransmitToken<'a>;

    fn capabilities(&self) -> DeviceCapabilities {
        let mut capabilities = DeviceCapabilities::default();
        capabilities.max_transmission_unit = 1500;
        capabilities.medium = Medium::Ip;
        capabilities.checksum = ChecksumCapabilities::default();
        capabilities
    }

    fn receive(&mut self, _timestamp: Instant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        self.rx.pop_front().map(|bytes| {
            (
                ReceiveToken { bytes },
                TransmitToken {
                    queue: &mut self.tx,
                },
            )
        })
    }

    fn transmit(&mut self, _timestamp: Instant) -> Option<Self::TxToken<'_>> {
        Some(TransmitToken {
            queue: &mut self.tx,
        })
    }
}

struct ReceiveToken {
    bytes: Vec<u8>,
}

impl smoltcp::phy::RxToken for ReceiveToken {
    fn consume<R, F>(self, function: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        function(&self.bytes)
    }
}

struct TransmitToken<'a> {
    queue: &'a mut VecDeque<Vec<u8>>,
}

impl smoltcp::phy::TxToken for TransmitToken<'_> {
    fn consume<R, F>(self, length: usize, function: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let mut bytes = vec![0; length];
        let result = function(&mut bytes);
        self.queue.push_back(bytes);
        result
    }
}

struct SmolPeer {
    device: QueueDevice,
    interface: Interface,
    sockets: SocketSet<'static>,
    tcp: SocketHandle,
}

impl SmolPeer {
    fn new() -> Self {
        let mut device = QueueDevice::default();
        let mut config = InterfaceConfig::new(HardwareAddress::Ip);
        config.random_seed = 0x5a17_cafe_1234_5678;
        let mut interface = Interface::new(config, &mut device, Instant::ZERO);
        interface.update_ip_addrs(|addresses| {
            addresses
                .push(IpCidr::new(IpAddress::Ipv4(SMOL_IP), 24))
                .unwrap();
        });
        let socket = TcpSocket::new(
            SocketBuffer::new(vec![0; 16 * 1024]),
            SocketBuffer::new(vec![0; 16 * 1024]),
        );
        let mut sockets = SocketSet::new(Vec::new());
        let tcp = sockets.add(socket);
        Self {
            device,
            interface,
            sockets,
            tcp,
        }
    }

    fn listen(&mut self) {
        self.socket().listen(SERVER_PORT).unwrap();
    }

    fn connect(&mut self) {
        let Self {
            interface,
            sockets,
            tcp,
            ..
        } = self;
        sockets
            .get_mut::<TcpSocket>(*tcp)
            .connect(
                interface.context(),
                (IpAddress::Ipv4(FIPS_IP), SERVER_PORT),
                CLIENT_PORT,
            )
            .unwrap();
    }

    fn poll(&mut self, now_ms: u64) {
        self.interface.poll(
            Instant::from_millis(i64::try_from(now_ms).unwrap()),
            &mut self.device,
            &mut self.sockets,
        );
    }

    fn socket(&mut self) -> &mut TcpSocket<'static> {
        self.sockets.get_mut(self.tcp)
    }

    fn state(&mut self) -> SmolState {
        self.socket().state()
    }

    fn send(&mut self, bytes: &[u8]) {
        assert_eq!(self.socket().send_slice(bytes).unwrap(), bytes.len());
    }

    fn receive(&mut self) -> Vec<u8> {
        let mut bytes = vec![0; 16 * 1024];
        let count = self.socket().recv_slice(&mut bytes).unwrap_or(0);
        bytes.truncate(count);
        bytes
    }

    fn close(&mut self) {
        self.socket().close();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Direction {
    FipsToSmol,
    SmolToFips,
}

#[derive(Clone, Copy, Debug)]
struct PacketFacts {
    direction: Direction,
    sequence: u32,
    syn: bool,
    ack: bool,
    fin: bool,
    payload_len: usize,
}

#[derive(Default)]
struct BridgeStats {
    ipv4_checksums_verified: usize,
    tcp_checksums_verified: usize,
    fips_options_injected: usize,
    fips_syn_options_verified: usize,
    observed: Vec<PacketFacts>,
    dropped: Vec<PacketFacts>,
}

struct Harness {
    fips: Stack<&'static str>,
    smol: SmolPeer,
    now_ms: u64,
    stats: BridgeStats,
}

impl Harness {
    fn new() -> Self {
        let config = FipsConfig {
            initial_rto_ms: 1_000,
            min_rto_ms: 200,
            ..FipsConfig::default()
        };
        Self {
            fips: Stack::new(config, 0x1234_5678_9abc_def0),
            smol: SmolPeer::new(),
            now_ms: 0,
            stats: BridgeStats::default(),
        }
    }

    fn advance(&mut self, milliseconds: u64) {
        self.now_ms += milliseconds;
    }

    fn step_with<F>(&mut self, mut drop_packet: F) -> usize
    where
        F: FnMut(PacketFacts) -> bool,
    {
        self.fips.poll(self.now_ms);
        self.smol.poll(self.now_ms);
        let from_fips = self.fips.drain_outbound();
        let from_smol = self.smol.device.tx.drain(..).collect::<Vec<_>>();
        let mut delivered = 0;

        for outbound in from_fips {
            assert_eq!(outbound.peer, FIPS_PEER);
            let segment = Segment::decode(&outbound.bytes).unwrap();
            let facts = packet_facts(Direction::FipsToSmol, &segment);
            trace_packet(facts, segment.ack);
            self.stats.observed.push(facts);
            if segment.flags.contains(Flags::SYN) {
                assert!(segment.supports_fips_version(FIPS_VERSION));
                self.stats.fips_syn_options_verified += 1;
            }
            if drop_packet(facts) {
                self.stats.dropped.push(facts);
                continue;
            }
            self.smol
                .device
                .rx
                .push_back(wrap_for_smoltcp(&outbound.bytes, FIPS_IP, SMOL_IP));
            delivered += 1;
        }

        for frame in from_smol {
            let bytes = unwrap_from_smoltcp(&frame, &mut self.stats);
            let segment = Segment::decode(&bytes).unwrap();
            let facts = packet_facts(Direction::SmolToFips, &segment);
            trace_packet(facts, segment.ack);
            self.stats.observed.push(facts);
            if drop_packet(facts) {
                self.stats.dropped.push(facts);
                continue;
            }
            self.fips.input(FIPS_PEER, &bytes, self.now_ms).unwrap();
            delivered += 1;
        }
        delivered
    }

    fn settle(&mut self) {
        for _ in 0..256 {
            if self.step_with(|_| false) == 0 {
                return;
            }
        }
        panic!("smoltcp oracle did not settle");
    }
}

fn packet_facts(direction: Direction, segment: &Segment) -> PacketFacts {
    PacketFacts {
        direction,
        sequence: segment.seq,
        syn: segment.flags.contains(Flags::SYN),
        ack: segment.flags.contains(Flags::ACK),
        fin: segment.flags.contains(Flags::FIN),
        payload_len: segment.payload.len(),
    }
}

fn trace_packet(facts: PacketFacts, acknowledgment: Option<u32>) {
    if std::env::var_os("FIPS_TCP_SMOLTCP_TRACE").is_some() {
        eprintln!("{facts:?} ack={acknowledgment:?}");
    }
}

fn wrap_for_smoltcp(tcp_fips: &[u8], source: Ipv4Address, destination: Ipv4Address) -> Vec<u8> {
    let mut tcp_bytes = tcp_fips.to_vec();
    TcpPacket::new_checked(&tcp_bytes).unwrap();
    TcpPacket::new_unchecked(&mut tcp_bytes)
        .fill_checksum(&IpAddress::Ipv4(source), &IpAddress::Ipv4(destination));

    let repr = Ipv4Repr {
        src_addr: source,
        dst_addr: destination,
        next_header: IpProtocol::Tcp,
        payload_len: tcp_bytes.len(),
        hop_limit: 64,
    };
    let mut frame = vec![0; repr.buffer_len() + tcp_bytes.len()];
    let mut packet = Ipv4Packet::new_unchecked(&mut frame);
    repr.emit(&mut packet, &ChecksumCapabilities::default());
    packet.payload_mut().copy_from_slice(&tcp_bytes);
    let packet = Ipv4Packet::new_checked(&frame).unwrap();
    assert!(packet.verify_checksum());
    assert!(
        TcpPacket::new_checked(packet.payload())
            .unwrap()
            .verify_checksum(&IpAddress::Ipv4(source), &IpAddress::Ipv4(destination),)
    );
    frame
}

fn unwrap_from_smoltcp(frame: &[u8], stats: &mut BridgeStats) -> Vec<u8> {
    let ipv4 = Ipv4Packet::new_checked(frame).unwrap();
    assert_eq!(ipv4.src_addr(), SMOL_IP);
    assert_eq!(ipv4.dst_addr(), FIPS_IP);
    assert_eq!(ipv4.next_header(), IpProtocol::Tcp);
    assert!(ipv4.verify_checksum());
    stats.ipv4_checksums_verified += 1;

    let tcp = TcpPacket::new_checked(ipv4.payload()).unwrap();
    assert!(tcp.verify_checksum(&IpAddress::Ipv4(SMOL_IP), &IpAddress::Ipv4(FIPS_IP),));
    stats.tcp_checksums_verified += 1;

    let mut bytes = ipv4.payload().to_vec();
    if tcp.syn()
        && !tcp
            .options()
            .windows(FIPS_OPTION.len())
            .any(|item| item == FIPS_OPTION)
    {
        let header_len = usize::from(tcp.header_len());
        assert!(header_len <= 56, "SYN options leave no room for TCP/FIPS");
        let option_offset = option_end_before_padding(tcp.options());
        bytes.splice(20 + option_offset..20 + option_offset, FIPS_OPTION);
        let mut packet = TcpPacket::new_unchecked(&mut bytes);
        packet.set_header_len(u8::try_from(header_len + FIPS_OPTION.len()).unwrap());
        stats.fips_options_injected += 1;
    }
    TcpPacket::new_unchecked(&mut bytes).set_checksum(0);
    bytes
}

fn option_end_before_padding(mut options: &[u8]) -> usize {
    let original_len = options.len();
    while !options.is_empty() && options[0] != 0 {
        let (remaining, _) = SmolTcpOption::parse(options).unwrap();
        options = remaining;
    }
    original_len - options.len()
}

#[test]
fn fips_client_recovers_syn_ack_data_and_fin_loss_against_smoltcp() {
    let mut harness = Harness::new();
    harness.smol.listen();
    let connection = harness
        .fips
        .connect_from_with_isn(FIPS_PEER, CLIENT_PORT, SERVER_PORT, 0x1020_3040, 0)
        .unwrap();

    let mut dropped_syn = None;
    harness.step_with(|facts| {
        if facts.direction == Direction::FipsToSmol && facts.syn && dropped_syn.is_none() {
            dropped_syn = Some(facts.sequence);
            true
        } else {
            false
        }
    });
    assert!(dropped_syn.is_some());
    harness.advance(1_000);
    harness.settle();
    assert_eq!(harness.fips.state(connection), Some(FipsState::Established));
    assert_eq!(harness.smol.state(), SmolState::Established);

    let request: Vec<u8> = (0..4096).map(|index| (index % 251) as u8).collect();
    assert_eq!(
        harness
            .fips
            .write(connection, &request, harness.now_ms)
            .unwrap(),
        request.len()
    );
    let mut dropped_data = None;
    harness.step_with(|facts| {
        if facts.direction == Direction::FipsToSmol
            && facts.payload_len > 0
            && dropped_data.is_none()
        {
            dropped_data = Some(facts.sequence);
            true
        } else {
            false
        }
    });
    assert!(dropped_data.is_some());
    harness.advance(10_000);

    let mut dropped_ack = false;
    for _ in 0..8 {
        harness.step_with(|facts| {
            if facts.direction == Direction::SmolToFips
                && facts.ack
                && facts.payload_len == 0
                && !dropped_ack
            {
                dropped_ack = true;
                true
            } else {
                false
            }
        });
        harness.advance(20);
    }
    assert!(dropped_ack);
    harness.advance(10_000);
    harness.settle();
    assert_eq!(harness.smol.receive(), request);

    let response: Vec<u8> = (0..3072).map(|index| (index % 239) as u8).collect();
    harness.smol.send(&response);
    harness.settle();
    assert_eq!(
        harness.fips.read(connection, 4096, harness.now_ms).unwrap(),
        response
    );

    harness.fips.close(connection, harness.now_ms).unwrap();
    let mut dropped_fin = None;
    harness.step_with(|facts| {
        if facts.direction == Direction::FipsToSmol && facts.fin && dropped_fin.is_none() {
            dropped_fin = Some(facts.sequence);
            true
        } else {
            false
        }
    });
    assert!(dropped_fin.is_some());
    assert_eq!(harness.smol.state(), SmolState::Established);
    harness.advance(10_000);
    harness.settle();
    assert_eq!(harness.smol.state(), SmolState::CloseWait);
    harness.smol.close();
    harness.settle();
    assert_eq!(harness.fips.state(connection), Some(FipsState::TimeWait));
    assert_eq!(harness.smol.state(), SmolState::Closed);

    assert!(harness.stats.fips_syn_options_verified >= 2);
    assert_eq!(harness.stats.fips_options_injected, 1);
    assert!(harness.stats.ipv4_checksums_verified > 0);
    assert_eq!(
        harness.stats.ipv4_checksums_verified,
        harness.stats.tcp_checksums_verified
    );
    for sequence in [
        dropped_syn.unwrap(),
        dropped_data.unwrap(),
        dropped_fin.unwrap(),
    ] {
        assert!(
            harness
                .stats
                .observed
                .iter()
                .filter(|facts| {
                    facts.direction == Direction::FipsToSmol && facts.sequence == sequence
                })
                .count()
                >= 2
        );
    }
}

#[test]
fn smoltcp_client_handshakes_and_closes_through_the_production_fips_stack() {
    let mut harness = Harness::new();
    harness.fips.listen(SERVER_PORT).unwrap();
    harness.smol.connect();
    harness.settle();

    let connection: ConnectionId = harness.fips.accept(SERVER_PORT).unwrap();
    assert_eq!(harness.fips.state(connection), Some(FipsState::Established));
    assert_eq!(harness.smol.state(), SmolState::Established);

    let from_smol = b"smoltcp initiated this connection";
    harness.smol.send(from_smol);
    harness.settle();
    assert_eq!(
        harness.fips.read(connection, 4096, harness.now_ms).unwrap(),
        from_smol
    );

    let from_fips = b"production TCP/FIPS replied";
    harness
        .fips
        .write(connection, from_fips, harness.now_ms)
        .unwrap();
    harness.settle();
    assert_eq!(harness.smol.receive(), from_fips);

    harness.smol.close();
    harness.settle();
    assert_eq!(harness.fips.state(connection), Some(FipsState::CloseWait));
    harness.fips.close(connection, harness.now_ms).unwrap();
    harness.settle();
    assert_eq!(harness.fips.state(connection), None);
    assert_eq!(harness.smol.state(), SmolState::TimeWait);

    assert_eq!(harness.stats.fips_options_injected, 1);
    assert_eq!(harness.stats.fips_syn_options_verified, 1);
    assert!(harness.stats.tcp_checksums_verified > 0);
}
