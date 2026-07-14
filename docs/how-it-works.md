# How literstream works

This is the plain-language tour. No prior knowledge of SQLite internals needed. Let us build up the ideas one at a time. 

literstream continuously copies a **live** SQLite database to object storage so
you can rebuild it later. You can restore to the latest version or as it looked at some earlier
moment. The trick is doing this *while the database is being written to*, without
slowing it down and without ever copying a half-written state. Here's how.

---

## The problem: backing up a moving target

You can't just `cp app.db backup.db` on a database that's being written; you'd
capture a torn, inconsistent snapshot. And copying the whole file every time
anything changes is hugely wasteful. We want to copy only *what changed*, and
only *complete* transactions.

SQLite gives us exactly the right tool for this: the **Write-Ahead Log**.

## Step 1: Put SQLite in WAL mode

In WAL (Write-Ahead Logging) mode, SQLite doesn't modify the main database file
on every write. Instead it appends the changed **pages** (fixed-size chunks of
the DB, usually 4 KB) to a side file called `app.db-wal`. A transaction is
"committed" when a special marker is appended to the WAL.

Two things make this perfect for replication:

1. **The WAL is append-only.** New changes go on the end, so we can read it
   forward like a log and never miss anything.
2. **It's a precise change-list.** Each WAL frame says "page N now looks like
   this." That's exactly the delta we want to ship, not the whole database.

Later, SQLite merges the WAL back into the main file in an operation called a
**checkpoint**. That's where the danger is (see Steps 3 and 6).

## Step 2: Take control of checkpointing

Normally SQLite checkpoints automatically whenever the WAL gets big. But a
checkpoint **resets the WAL**: once frames are merged into the main file, SQLite
is free to overwrite them with new writes. If that happens *before* literstream
has copied those frames out, they're gone resulting in a hole in our backup.

So the very first thing literstream does is **turn off SQLite's automatic
checkpointing** (`PRAGMA wal_autocheckpoint = 0`). From now on, *literstream*
decides when checkpoints happen. It triggers it always *after* it has safely 
replicated the frames in question.

It also sets `PERSIST_WAL` so the `-wal` file sticks around even when
connections close, and it leaves your application's durability setting
(`synchronous`) untouched. This makes sure that literstream never 
silently weakens your guarantees.

## Step 3: Pin the WAL with a long-lived read transaction

Disabling *automatic* checkpoints isn't quite enough. Another connection (or a
manual checkpoint) could still reset the WAL out from under us. literstream needs
a guarantee that **the WAL can't be rewound while it's reading it.**

SQLite provides this through readers. When you open a read transaction, SQLite
records a **read mark**. It is a promise that the WAL frames that reader can see won't
be recycled until it finishes. A checkpoint that would reset the WAL will refuse
to (or block) as long as a reader is holding an earlier mark.

So literstream holds a **long-lived read transaction** against a tiny bookkeeping
table (`BEGIN; SELECT ... FROM _literstream_seq`). This "pins" a spot in the WAL.
While that read transaction is open, no checkpoint can pull the rug out. Just
before literstream runs its *own* controlled checkpoint, it releases the pin, and
re-acquires it right after.

> **Why a real table row?** Reading `sqlite_master` alone leaves a read mark of
> zero, which does *not* block a WAL reset. Reading a committed row from a real
> table is what actually holds the mark. (This is one of the subtle lessons
> inherited from Litestream.)

## Step 4: Turn WAL frames into LTX files

Now the copying. Each time literstream syncs:

1. It reads the WAL from where it left off to the latest committed transaction.
2. It collects the newest version of each changed page (a page written twice in
   that span only ships once).
3. It packages those pages into an **LTX file**. LTX is a compact, self-describing,
   checksummed container. Each file covers a range of transaction IDs.

The **first** sync writes a full **snapshot**, every page in the database, as
one LTX file. Every sync after that writes an **incremental**, only the pages
that changed. LTX files are named by their transaction range and are
**immutable**: once written, they're never modified.

> LTX is Litestream's format (from `superfly/ltx`). literstream writes it
> byte-for-byte the same way, which is why the two tools can restore each other's
> backups.

## Step 5: Write to the object store atomically

The LTX bytes are uploaded to the object store under a predictable key like
`app/0000/<txid-range>.ltx` (the level as a 4-digit hex directory, matching
litestream's remote layout). Two safety rules apply:

- **One writer.** literstream takes a host-local lock on the database file, so
  two processes can't try to replicate the same database at once.
- **Compare-and-swap.** Uploads use an **if-not-exists** conditional write. If a
  file for that transaction range already exists, literstream compares bytes:
  identical means it's a harmless retry; *different* means another writer
  produced conflicting history ("split-brain") and literstream refuses to
  overwrite it. This is the guarantee that a replica can't be
  silently corrupted.

Replication position only advances *after* a successful upload, so a failed
upload is simply retried from the same spot.

## Step 6: Checkpoint without leaving a gap

Once the new frames are safely in the object store, literstream **checkpoints**
the WAL, merging them into the main database file.

Here's the subtle part. A non-blocking (PASSIVE) checkpoint runs alongside your
writes, so it can merge a *just-committed* frame into the main file before
literstream has replicated it. Once the WAL resets, that frame lives only in the
DB file, invisible to the next incremental read. A silent hole. (A real bug we
hit.)

literstream follows Litestream's rule here: **never stall the writer on the happy
path.** So instead of *excluding* the race, it *notices* it. Around a checkpoint
it does three things:

1. **Capture first, upload later (a shadow).** It builds the pending LTX from the
   WAL (a fast, local step) *before* checkpointing, and holds those bytes in
   memory. Then it checkpoints, then it uploads. Because the captured frames
   outlive the WAL reset (we're holding them), the checkpoint can't lose them, and
   the only window where a frame could slip past unrecorded shrinks from a network
   upload down to a local build.
2. **Restart the WAL (seq-bump).** Right after a PASSIVE checkpoint that drained
   the WAL, while no read-mark is held to block it, it writes one row to
   `_literstream_seq`, forcing SQLite to *restart* the WAL into a fresh generation
   (reusing the file instead of extending it) and seeding a real frame for the next
   read-mark to pin.
3. **Detect the rest.** It still compares what the checkpoint merged into the DB
   against what it captured; if a write did land in that tiny window, the next sync
   **re-snapshots** from the DB to recover it. Correctness by noticing, never a
   stall.

The capture-first step makes those re-snapshots *rare* (only a write in the
build-sized window), not one per checkpoint.

> **Trade-off, stated honestly.** This is Litestream's cost profile: the app is
> never blocked, and the price is a rare re-snapshot when a checkpoint outruns
> replication. The shadow here is *in-memory* (it covers a single checkpoint, not
> a process restart); under *sustained* writes PASSIVE keeps returning busy and the
> WAL can still grow on disk (Litestream has the same property, so monitor disk),
> resetting in idle windows. A *persistent* shadow WAL would also survive restarts,
> a further step this port doesn't take.

## Step 7: Compaction

If we only ever wrote one small file per transaction, restoring would eventually
mean replaying millions of tiny files. So literstream **compacts**, exactly like
Litestream, using tiered, time-based **levels**:

- **Level 0** is the raw stream, one file per sync.
- **Higher levels** merge the level below them on a schedule (by default: L0→L1
  every 30s, L1→L2 every 5m, L2→L3 every 1h). Merging keeps only the latest
  version of each page, so files get fewer and larger as they age.
- **Level 9** holds periodic **full snapshots**: a complete database image that
  serves as a restore anchor.

Merging is just combining LTX files into a bigger LTX file, so everything stays
byte-compatible.

## Step 8: Retention

Compaction bounds the *number* of files; **retention** bounds their *age*. Old
files are deleted once they've been safely merged into a higher level and are
older than a retention window (L0 files are kept a few minutes, snapshots for a
day, by default). One rule is sacred: **the newest file at every level is always
kept**, so the chain is never broken.

Retention is what makes point-in-time recovery a *tradeoff* rather than free:
inside the retention window every individual transaction is restorable; once a
point ages out, restoring to it snaps to the nearest surviving (coarser)
boundary.

## Step 9: Restoring

Restoring reverses the whole process and needs nothing but the object store:

1. **List** every LTX file across all levels.
2. **Plan** the shortest chain that covers transaction 1 up to the target,
   preferring the biggest (most-compacted) files. Fine-grained points still in
   Level 0 are used when available, so a recent snapshot never hides them.
3. **Apply** the files in order into a blank image: lay down the snapshot, then
   replay each increment's changed pages on top.

The result is a byte-perfect database file you can open directly.

- **Latest:** `restore()` the newest state.
- **By transaction:** `restore_to_txid(n)` as of a specific commit.
- **By time:** `restore_to_timestamp(ms)` snaps to the newest transaction at or
  before a wall-clock instant.

There's also a `ReplicaReader` that fetches *individual pages* straight from the
replica with ranged reads. This is what enables reading a database directly from
object storage without downloading all of it.

## The `Driver` ties it together

You could call each step yourself, but the `Driver` runs the whole schedule from
a single `tick(now)`:

```
each tick:  sync → checkpoint (if WAL is big) → compact levels (on their intervals)
            → snapshot (on its interval) → enforce retention
```

Give it a wall-clock time and it does the right thing on the right cadence, the
library equivalent of Litestream's background loop.

---

### Recap in one breath

Open SQLite in WAL mode, stop it from checkpointing on its own, and hold a read
transaction so the WAL can't be reset underneath you. Read the new WAL frames,
pack them into immutable LTX files, and write them to the object store with an
atomic if-not-exists upload. Merge and expire old files on a schedule so storage
stays bounded. To recover, replay the file chain back into a database image. You can restore to the
latest version or as of any point still inside your retention window.
