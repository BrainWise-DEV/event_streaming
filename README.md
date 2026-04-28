# Event Streaming

Event Streaming enables inter site communications between two or more sites. You can subscribe to Document Types and stream Documents between different sites.

For Example: Consider you have a main office for accounting and several branch offices that handle sales. To keep everything in sync via Event Streaming:

- Sync Master Data: Set up the branches to "follow" the main office so they always have the latest Item, Customer, and Supplier lists.

- Automate Postings: Set up the main office to "follow" the branches so that every time a Sales Invoice is created locally, it’s automatically sent to the main ledger.

---

## What it does

- Replicates inserts, updates, cancels, and deletes for any DocType you opt in to.
- Updates ship as **diffs** (only changed fields and child-row deltas), not full payloads.
- Replay uses the standard Frappe document lifecycle on the consumer (`insert`, `save`,
  `delete`) so all controller hooks, validations, and downstream side-effects run as if
  a local user had made the change.
- Pulls in **link-target dependencies** automatically — if a Sales Invoice references
  a Customer that doesn't exist on the consumer yet, the consumer fetches it from the
  producer before inserting the invoice.
- Supports per-row authorization conditions, cross-DocType mapping, and selective
  re-naming. Failures land in a Sync Log with a Resync button.

---

## Concepts

### Producer and Consumer

Two sites, two DocTypes:

| Site | DocType  | Holds |
|---|---|---|
| Consumer | `Event Producer` | the URL of the upstream site, API key/secret, the list of DocTypes this site is subscribing to, and a `last_update` timestamp watermark. |
| Producer | `Event Consumer` | one row per registered downstream site, with the DocTypes it asked for and per-DocType approval status. |

Note the inversion: when site **B** wants to receive data from site **A**, you create
an `Event Producer` record on site B (pointing at A). That action registers an
`Event Consumer` record on site A automatically — no manual setup required on the
producer.

### Event Update Log

The producer's write log. On every save/cancel/delete of a subscribed DocType, an
`Event Update Log` row is inserted with:

- `ref_doctype`, `docname` — what changed,
- `update_type` — `Create` / `Update` / `Cancel` / `Delete`,
- `data` — the full document JSON for `Create`, or a structured diff
  (`{changed, added, removed, row_changed}`) for `Update`.

Consumers pull from this log and acknowledge each entry via `Event Update Log Consumer`
child rows on the log itself.

### Event Sync Log

Append-only audit on the consumer: one row per replay attempt with status
(`Synced` / `Failed`), the producer URL, the doctype/name, the data, and the traceback
on failure. Open a failed row, press **Resync**, and `event_streaming` will replay it.

---

## Setup

On the **producer** site:

1. Make sure the user that the consumer will authenticate as has API keys generated
   (User form → Settings → API Access).

On the **consumer** site:

1. Create a new `Event Producer` record. Fill in:
   - **Producer URL** — the upstream site (e.g. `https://producer.example.com`).
   - **User** — the user whose API key/secret you'll use to authenticate.
   - **API Key** / **API Secret** — those credentials.
   - **Producer DocTypes** child table — one row per DocType you want to receive,
     with optional `condition`, `mapping`, and `use_same_name` knobs (see below).

2. Save. The consumer immediately calls `register_consumer` on the producer, which
   creates an `Event Consumer` row there and seeds the watermark.

3. Approve the subscription on the producer side. Open the `Event Consumer` record
   on the producer; for each DocType row, set `status = "Approved"`. Until approved,
   the producer ignores writes for that DocType (no `Event Update Log` is generated).

---

## How replication runs

![alt text](<event streaming.png>)

The webhook ping is just a nudge ("there's news for you"). The actual data is pulled by
the consumer over HTTP via `FrappeClient`. If the consumer is offline when the producer
pings, the next pull will pick up everything since the stored watermark — no events are
lost.

---

## Naming policy: `use_same_name`

Per producer-DocType row in `Event Producer`:

| `use_same_name` | What lands in the consumer's table |
|---|---|
| **True** | Document inserted with the producer's exact `name` (e.g. `ACC-SINV-2026-00012` on both sides). |
| **False** | Consumer assigns its own local `name`. The producer's name is preserved in two read-only custom fields auto-installed on the consumer side: `remote_docname` and `remote_site_name`. |

Future updates resolve the local copy via either the shared name or the
`remote_docname` lookup, so update diffs reach the right row regardless of which
naming policy you picked.

---

## Selective subscription: `condition`

Each `Event Consumer Document Type` row on the producer can carry a `condition`. The
producer evaluates it for every candidate update; only rows where the condition is
true are returned to that consumer.

- **Python expression**: `doc.customer_group == "Wholesale"` — evaluated via
  `frappe.safe_eval` against the document.
- **Custom callable**: `cmd:my_app.access.is_visible_to_branch` — the producer calls
  the named function with `consumer`, `doc`, `update_log` kwargs.

When a row's access flips from false to true (e.g. its status changed), the consumer
also receives any **previously-skipped** logs for that document on its next pull, so
its local copy can be brought up to date.

---

## Cross-DocType mapping

`Document Type Mapping` maps a producer DocType to a different consumer DocType
field-by-field. Useful for setups like *producer's `Sales Order` becomes consumer's
`Purchase Order`*. On the consumer, the relevant `Event Producer` row sets
`has_mapping = 1` and points at the mapping. Replication then translates each payload
through the mapping before insert/save.

Both `Document Type Mapping` and `Document Type Field Mapping` (its child) are part
of the app.

---

## Failure handling

If a `sync()` call raises (validation error on the consumer, missing dependency that
couldn't be fetched, link target the consumer forbids), the failure is logged in
`Event Sync Log` with the full traceback and `status = "Failed"`. Open the row in the
desk and click **Resync** to reattempt. The producer's watermark is *not* advanced for
failed updates, but the failure does not block subsequent pulls — the consumer keeps
processing other logs and only the failing one needs intervention.

---

## File map

```
event_streaming/event_streaming/
├── doctype/
│   ├── event_producer/                  – consumer-side: subscription record
│   ├── event_producer_document_type/    – child table: which DocTypes to receive
│   ├── event_producer_last_update/      – per-producer watermark
│   ├── event_consumer/                  – producer-side: registered consumer
│   ├── event_consumer_document_type/    – child table: which DocTypes are approved
│   ├── event_update_log/                – producer-side: the write log
│   ├── event_update_log_consumer/       – child rows: which consumer read which log
│   ├── event_sync_log/                  – consumer-side: replay attempts (Resync UI)
│   ├── document_type_mapping/           – cross-DocType field translation
│   └── document_type_field_mapping/     – child table: per-field translation rules
└── hooks.py                             – wildcard doc_events that fire notify_consumers
```

Key entry points worth reading first:

- `event_update_log/event_update_log.py` — `notify_consumers()` (producer-side hook)
  and the diff builder `get_update()`.
- `event_consumer/event_consumer.py` — `register_consumer()` (subscription handshake)
  and `notify()` (the webhook ping).
- `event_producer/event_producer.py` — `pull_from_node()` and the `sync()` /
  `set_insert()` / `set_update()` / `set_delete()` replay path; `sync_dependencies()`
  for recursive link-target fetching.
