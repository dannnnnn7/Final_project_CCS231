# Sari-Sari Store Restock & Lista Alert System (Pub-Sub Middleware)

A local publish-subscribe (pub-sub) middleware system built with **Python 3 (`asyncio`, `sqlite3`, `json`)** modeled after **Sari-Sari Store**. It supports hierarchical topic wildcards (`+` and `#`), persistent SQLite WAL queues for offline catch-up and at-least-once delivery, and subscriber-side idempotency (`_seen.db`).

## Project Files
- `protocol.py` — Newline-delimited JSON wire frames, topic wildcard matching (`+` and `#`), and color-coded terminal helpers.
- `broker.py` — Central store hub (`PubSubBroker`) and SQLite WAL durable queue (`DurableStore`).
- `publisher.py` — Event sender (`PublisherClient`) supporting both JSON `--payload` and simple flags (`--item`, `--packs`, `--cases`, `--amount`).
- `subscriber.py` — Event listener (`SubscriberClient`) with topic filters, `ACK` confirmations, and SQLite idempotency (`<client_id>_seen.db`).
- `demo.py` — Automated end-to-end test covering topic filtering, offline queuing, reconnect catch-up, and delivery assertions.
- `requirements.txt` — Dependency note (uses Python standard library only).
- `.gitignore` — Ignores SQLite database/WAL files, cache folders, and editor files.

## Scenario & Clients
- **Publishers:**
  - `shelf_scanner_pub` — Low stock, out-of-stock, and restocked shelf events.
  - `counter_lista_pub` — Customer *lista* (credit tab) payments and limit alerts.
- **Subscribers:**
  - `restock_runner_sub` — `store/+/low_stock`, `store/+/out_of_stock`
  - `lista_watch_sub` — `store/lista_tita_rosa/#`, `store/lista_kuya_ben/#`
  - `store_audit_sub` — `store/#`

## Quick Start in VS Code (PowerShell)

### Fastest Way: Run the Automated Demo
```powershell
python demo.py
