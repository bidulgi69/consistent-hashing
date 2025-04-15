# Consistent Hashing

A lightweight Java simulation of **consistent hashing** with **virtual nodes (vnode)**, designed to demonstrate rebalancing logic, token ring mechanics, and ownership models on a virtual topology — all without real networking.

---

## Project Objectives

- Build a consistent hashing ring and explore its internal mechanics
- Support **node join** and **node leave** operations with data **rebalance logic**
- Simulate ownership of key ranges using **vnode/token placement**

---

## Features

- Consistent hashing ring with virtual nodes
- Clockwise ownership semantics
- Handling wrap-around ranges, metadata sync, responsibility lookups

---

## Rebalancing Behavior

### Node Joining (BOOTSTRAPING)
- New node introduces vnodes (tokens) to the ring
- Each vnode claims a range `[token, nextToken)`
- Range data is scanned from `getNode(nextToken)` — previous owner

### Node Leaving (LEAVING)
- Leaving vnode owned `[prevToken, token)`
- Data in this range is scanned and handed off to `getNode(token)` — new owner
- Wrap-around ranges (e.g., `[900, 100)`) are handled via split scanning

