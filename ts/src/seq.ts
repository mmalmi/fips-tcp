export const u32 = (value: number): number => value >>> 0;

export const before = (left: number, right: number): boolean => ((left - right) | 0) < 0;

export const after = (left: number, right: number): boolean => before(right, left);

export const beforeOrEqual = (left: number, right: number): boolean => left === right || before(left, right);

export const inClosedInterval = (value: number, start: number, end: number): boolean =>
  !before(value, start) && !after(value, end);

export const distance = (start: number, end: number): number => (end - start) >>> 0;
