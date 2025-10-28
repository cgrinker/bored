# B+Tree Page Layout Draft

## Goals
- Establish the initial on-disk layout for B+Tree internal and leaf pages.
- Define version gating so future layout revisions remain backward compatible.
- Capture questions and open items blocking Milestone 0 tasks.

## Page Types
- **Internal page:** stores ordered key pivots plus child page identifiers.
- **Leaf page:** stores ordered key payloads plus heap tuple references (row ids or record ids).

## Common Page Header (16 bytes)
| Field | Size | Description |
| ----- | ---- | ----------- |
| `magic` | 2 bytes | Distinguishes index pages from heap/fsm pages (`0x4942`). |
| `version` | 2 bytes | Layout version (start at `1`). |
| `flags` | 2 bytes | Bitfield for leaf/internal, pending split, etc. |
| `level` | 2 bytes | Tree level (0 == leaf). |
| `parent_page_id` | 4 bytes | Optional parent reference for diagnostics (0 when unknown). |
| `right_sibling_page_id` | 4 bytes | Right-link for leaf scans and split chaining. |

See `bored/storage/index_btree_page.hpp` for the concrete header struct and related helpers.

## Slot Directory Layout
- Slot directory grows from the end of the page backwards.
- Each slot entry is 4 bytes: `[2 bytes key_offset][2 bytes key_length]` (`IndexBtreeSlotEntry`).
- Internal pages reserve the high bit of `key_length` to indicate an "infinite" pivot (rightmost fence key). The mask lives in `kIndexBtreeSlotInfiniteKeyMask`.

## Payload Area
- Grows from the header forward.
- Internal payload encodes `IndexBtreeChildPointer` (4-byte page id) preceding key bytes to keep pointers close to associated key.
- Leaf payload encodes `IndexBtreeTuplePointer` (page id + slot id) followed by optional inlined key payload for covering indexes.

## Version Gates
- `version` guards structural changes. Future revisions bump the version and document compatibility expectations here.
- `flags` bit assignments (reflected in `kIndexPageFlag*` constants):
  - `0x0001` -> leaf page.
  - `0x0002` -> pending split (used by WAL replay to detect half-complete splits).
  - `0x0004` -> compressed keys.

## Outstanding Questions
- Should we reserve additional header space for checksum or LSN? (Current plan relies on PageManager header.)
- Do we need per-page high-key storage for fast right-link verification? (Leaning yes; to decide during Milestone 0 coding.)
- Confirm whether tuple pointers target heap page/slot or row id abstraction.

## WAL Record Shapes (Milestone 0)
- `WalIndexSplitHeader` + payload captures left/right slot directories, payload bytes, and separator key so recovery can rebuild both children and parent metadata after a split. Flags indicate root vs. leaf splits and parent slot insertion point.
- `WalIndexMergeHeader` records the merged page layout, removed sibling, and separator key removed from the parent to support logical rollbacks of delete-driven merges.
- `WalIndexBulkCheckpointHeader` snapshots bulk build progress, including per-run state (`WalIndexBulkRunEntry`) and stage/flag markers for resumable bulk loading.

## Next Steps
- Finalise header and slot directory structure in code (`src/storage/index` TBD).
 - Wire split/merge WAL record encoding into the index write path once page management is live.
- Derive sizing tests to ensure slot directory and payload layout meet alignment requirements.
