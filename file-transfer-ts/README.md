# TypeScript file transfer

Recipient-approved, resumable file transfers over authenticated FIPS streams.
This package shares the file-transfer protocol with the Rust implementation.

Use Node.js 26.13.0 and pnpm 10.24.0. The checked-in lockfile pins the complete
development dependency graph and package integrity hashes.

```sh
cd file-transfer-ts
pnpm install --frozen-lockfile --ignore-scripts
pnpm check
pnpm test
pnpm build
pnpm pack --pack-destination ./artifacts
```

The test suite includes the canonical Rust/TypeScript manifest and control
vectors in `../protocol/file-transfer-v2-vectors.json`. Release qualification
also requires fresh bidirectional live interoperability with the native
implementation. Passing the TypeScript suite alone does not satisfy that gate.

Iris Chat consumes this package through its existing vendor archive. A release
must bind the archive, every included source/build file, the exact public source
commit, lockfile, toolchain, and qualification evidence. No npm registry release
or change to the Rust package version is implied by this delivery path.
