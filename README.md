# Assigning Block Producers to Shards (PoC)

This repo demonstrates the proposed algorithm for assigning block
proposers to shards. It is relevant to 
[NEP #76](https://github.com/nearprotocol/NEPs/issues/76).

The important function is `assign_shards` in `src/lib.rs`. There are
a few tests in the same file showing how well the algorithm balances
number of validators and stakes per shard in different cases.