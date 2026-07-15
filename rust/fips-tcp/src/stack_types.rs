#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ConnectionKey<P> {
    peer: P,
    local_port: u16,
    remote_port: u16,
}
