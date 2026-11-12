# What a sync makes durable

`FileHandle.sync`, `FileHandle.syncData` and `syncDir` are the calls
the rest of egres relies on for durability. This file records what
ext4 and XFS do with them, measured on real filesystems, and the rules
the code follows because of it.

## The test

Measured on 2026-11-11: Linux 7.0.0-38 (Ubuntu 26.04), e2fsprogs
1.47.2, xfsprogs 6.18.0, default mkfs and mount options.

Each filesystem is made on a loop file with a `dm-flakey` target under
it, the method xfstests uses. A power cut is: the target starts
dropping every write, the filesystem is unmounted, the target is set
to work again, the filesystem is mounted. Only what had reached the
device before the cut is there afterwards. For a failed sync the
target fails every write instead of dropping it.

Every case starts in a directory that is already durable.

The same cases were run on overlayfs, with its lower, upper and work
directories on ext4 and then on XFS. That is where a Docker container
writes when its data directory is not on a volume. Every result was
the one of the filesystem underneath, also in a directory that came
from the lower layer, so the tables below have no column for it.

These are the filesystems the rules were measured on. No other one
has been tested.

## Which names survive a power cut

| Steps before the cut                             | ext4      | XFS       |
|--------------------------------------------------|-----------|-----------|
| create, write, no sync                           | no file   | no file   |
| create under the final name, write, fsync(file)  | file      | file      |
| create under the final name with O_DSYNC, write  | file      | file      |
| tmp, fsync(tmp), rename                          | tmp only  | tmp only  |
| the same, renamed over an existing file          | old bytes | old bytes |
| tmp, fsync(tmp), rename, fsync(renamed file)     | renamed   | renamed   |
| tmp, fsync(tmp), rename, fsync(directory)        | renamed   | renamed   |

- A file created under its final name is durable with its own sync,
  name included. POSIX does not promise that; both filesystems do it.
- A rename is not durable until something after it is synced. Without
  that, the cut brings the old name back, and the old bytes when the
  rename replaced a file.

## What a failed sync leaves behind

| Steps                                  | ext4            | XFS            |
|----------------------------------------|-----------------|----------------|
| overwrite a durable file, then:        |                 |                |
| fdatasync while writes fail            | EIO             | EIO            |
| fdatasync again, writes work           | success         | success        |
| a read of those bytes from the cache   | new bytes       | new bytes      |
| the same read, the cache dropped       | old bytes       | old bytes      |
| after a cut the file has               | old bytes       | old bytes      |
| tmp, fsync(tmp), rename, then:         |                 |                |
| fsync(directory) while writes fail     | EIO             | EIO            |
| fsync(directory) again, writes work    | success         | success        |
| after a cut the directory has          | tmp only        | tmp only       |

- The sync after a failed one reports success, and what the first one
  failed to write is not on the device. This holds for a file's bytes
  and for a directory's names.
- After a failed sync a read returns the new bytes while the kernel
  still has the page and the old ones once it has let the page go.
- A new file whose sync failed can be on the device later with its
  name and size and without its bytes, once another sync has written
  the journal: 8192 bytes that are not the ones written on ext4, an
  empty file on XFS.

## Which failed sync stops the filesystem

Each step below failed once with EIO on a device that failed every
write. The device then worked again and every kind of op was tried.
These cases were not run on overlayfs.

| The step that failed                              | ext4      | XFS       |
|---------------------------------------------------|-----------|-----------|
| fsync of a new file                               | works on  | works on  |
| fdatasync after an append or an overwrite         | works on  | works on  |
| O_DSYNC write inside a file or past its end       | works on  | works on  |
| the same writes with O_DIRECT                     | works on  | works on  |
| fsync(directory) after a rename                   | read-only | shut down |
| fsync(directory) after a create                   | read-only | shut down |
| fsync(directory) after a delete                   | read-only | shut down |

A failed write of a file's bytes fails that sync and nothing else. A
failed write of the journal, which is what a directory's sync is,
stops the whole filesystem: ext4 aborts its journal and goes
read-only, XFS shuts down. From then until it is unmounted and
mounted again:

| Op on the stopped filesystem                      | ext4      | XFS       |
|---------------------------------------------------|-----------|-----------|
| fsync(directory), the next op after the failure   | success   | success   |
| fsync(directory), later                           | EROFS     | success   |
| read, list a directory, stat                      | works     | EIO       |
| open a file for reading                           | works     | EIO       |
| write, also with O_DSYNC or O_DIRECT              | EROFS     | EIO       |
| fdatasync                                         | EROFS     | EIO       |
| create, open for writing, mkdir                   | EROFS     | EIO       |
| rename, delete, truncate, fallocate, set mtime    | EROFS     | EIO       |

After the unmount and the mount both work again. They have the names
and bytes from before the failed sync and nothing from after it.

## The rules in the code

1. A rename is followed by `syncDir` on its directory before anything
   depends on the new name: the raft snapshot
   (`MetaStore.setSnapshot`) and the WAL file prepared ahead of a
   rotation (`NextLog.rename`).
2. A file created under its final name gets `syncDir` as well: an SST
   (`Store.sync`, once per batch of puts), the WAL file created at a
   rotation and the raft state file. On ext4 and XFS its own sync
   would do; other filesystems need not behave so.
3. An SST is written under its final name, with no rename. Nothing
   needs the name to appear only once the file is whole: no reader
   opens a table before the manifest lists it, and
   `Forest.removeOrphans` deletes what a crash left half written.
4. A failed sync is never asked again by itself. `sync`, `syncData`
   and `syncDir` return the error. The caller writes the bytes again
   and then syncs (a flush or a compaction puts its tables again, the
   WAL writes the whole item again), or it gives the file up.
5. Nothing acts on a name before its `syncDir` returned without error:
   no manifest change names the table, no file is deleted for the
   snapshot, no entry is appended to the log file.

## The simulated disk

`zfiber/simdisk.zig` is stricter than both filesystems on names: only
a directory's sync makes a name durable, a file's own sync never does.

`Faults.sync_drop` is the failed sync of a file above: the pending
writes still read back, the device does not have them, and a later
sync does not write them. The disk never gives the old bytes back to
a read before a crash, which the kernel may.

`Faults.shutdown` is the failed sync of a directory: the filesystem
stops as one of the two did, drawn from the seed (`Shut`), for the
tree the rates are set on and until its `crash`. A directory's sync
on it reports success and lands nothing.

`Faults.sync` on a directory is kinder than either: the names stay
pending, a second sync lands them, and the disk goes on working.
Neither filesystem did that for a failed write of the journal.
