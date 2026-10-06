use fips_tcp::{Config, Stack};

pub(crate) struct Pair {
    pub a: Stack<String>,
    pub b: Stack<String>,
    pub now: u64,
}

impl Pair {
    pub fn new(config: Config) -> Self {
        Self {
            a: Stack::new(config.clone(), 0x1111_2222_3333_4444),
            b: Stack::new(config, 0xaaaa_bbbb_cccc_dddd),
            now: 0,
        }
    }

    pub fn step_with<F>(&mut self, mut transform: F) -> usize
    where
        F: FnMut(bool, Vec<u8>) -> Vec<Vec<u8>>,
    {
        self.a.poll(self.now);
        self.b.poll(self.now);
        let from_a = self.a.drain_outbound();
        let from_b = self.b.drain_outbound();
        let mut delivered = 0;
        for outbound in from_a {
            assert_eq!(outbound.peer, "b");
            for bytes in transform(true, outbound.bytes) {
                self.b.input("a".to_string(), &bytes, self.now).unwrap();
                delivered += 1;
            }
        }
        for outbound in from_b {
            assert_eq!(outbound.peer, "a");
            for bytes in transform(false, outbound.bytes) {
                self.a.input("b".to_string(), &bytes, self.now).unwrap();
                delivered += 1;
            }
        }
        delivered
    }

    pub fn settle(&mut self) {
        for _ in 0..256 {
            if self.step_with(|_, bytes| vec![bytes]) == 0 {
                return;
            }
        }
        panic!("pair did not settle");
    }

    pub fn advance(&mut self, millis: u64) {
        self.now += millis;
    }
}
