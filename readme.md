# A library adding indexing capabilities to wnfs

The block store is a file based one that stores each block in a file named after the CID under `<roo-dir>/blockstore/`.

The state needed to re-use the store after a shutdown is made of:
- `<roo-dir>/access.key` : the access key of the root directory.
- `<roo-dir>/forest.cid` : the CID of the stored forest.

A simple command line interface is available in `examples/cli.rs`. Available commands are:

- `cargo run --example cli -- put <filename>` to import a file.
- `cargo run --example cli -- get <filename>` to retrieve a file and display it as utf-8.
- `cargo run --example cli -- ls` to list the files imported.
