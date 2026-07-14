export const u32 = (value) => value >>> 0;
export const before = (left, right) => ((left - right) | 0) < 0;
export const after = (left, right) => before(right, left);
export const beforeOrEqual = (left, right) => left === right || before(left, right);
export const inClosedInterval = (value, start, end) => !before(value, start) && !after(value, end);
export const distance = (start, end) => (end - start) >>> 0;
