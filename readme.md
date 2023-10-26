# A library adding indexing capabilities to wnfs

The block store is a file based one that stores each block in a file named after the CID under `<roo-dir>/blockstore/`.

The state needed to re-use the store after a shutdown is made of:

- `<roo-dir>/access.key` : the access key of the root directory.
- `<roo-dir>/forest.cid` : the CID of the stored forest.

A simple command line interface is available in `examples/cli.rs`. Available commands are:

- `cargo run --release --example cli -- put <filename>` to import a file.
- `cargo run --release --example cli -- get <filename>` to retrieve a resource and display its default variant as utf-8.
- `cargo run --release --example cli -- ls` to list the resources imported.
- `cargo run --release --example cli -- search <text>` to retrieve resources matching <text>.
