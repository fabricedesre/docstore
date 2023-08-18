This is a simple test program to understand how to use wnfs private storage.

The block store is a file based one that stores each block in a file named after the CID under `blockstore/`.

The state needed to re-use the store after a shutdown is made of:
- `access.key` : the access key of the root directory.
- `forest.cid` : the CID of the stored forest.

When running without parameters, a store will be created either from scratch or reusing an existing one, and the list of files stored will be printed.

Available parameters:
- `cargo run -- put <filename>` to add a file.
- `cargo run -- get <filename>` to retrieve a file and display it as utf-8.
