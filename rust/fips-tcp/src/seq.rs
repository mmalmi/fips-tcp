//! TCP's wrapping 32-bit byte sequence arithmetic.

pub(crate) fn before(left: u32, right: u32) -> bool {
    (left.wrapping_sub(right) as i32) < 0
}

pub(crate) fn after(left: u32, right: u32) -> bool {
    before(right, left)
}

pub(crate) fn before_or_equal(left: u32, right: u32) -> bool {
    left == right || before(left, right)
}

pub(crate) fn in_closed_interval(value: u32, start: u32, end: u32) -> bool {
    !before(value, start) && !after(value, end)
}

pub(crate) fn distance(start: u32, end: u32) -> usize {
    end.wrapping_sub(start) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn comparisons_cross_wrap() {
        let before_wrap = u32::MAX - 4;
        let after_wrap = 7;
        assert!(before(before_wrap, after_wrap));
        assert!(after(after_wrap, before_wrap));
        assert_eq!(distance(before_wrap, after_wrap), 12);
        assert!(in_closed_interval(0, before_wrap, after_wrap));
    }
}
