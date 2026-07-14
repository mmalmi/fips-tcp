import { readdirSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { expect, test } from "vitest";

test("TypeScript source files stay below five hundred lines", () => {
  for (const directory of ["../src", "."]) {
    const source = fileURLToPath(new URL(directory, import.meta.url));
    for (const name of readdirSync(source).filter((entry) => entry.endsWith(".ts"))) {
      const lines = readFileSync(`${source}/${name}`, "utf8").split(/\r?\n/).length - 1;
      expect(
        lines,
        `${name} has ${lines} lines; TypeScript source files are limited to 500`,
      ).toBeLessThanOrEqual(500);
    }
  }
});
