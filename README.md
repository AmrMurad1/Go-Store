# LSM Tree Database

A simple key-value database implementation using LSM (Log-Structured Merge) Trees in Go.

## What is this?

Educational project to understand how modern databases like Cassandra, RocksDB, and LevelDB work internally.

## Features

- **Memtable**: In-memory skiplist for fast writes
- **Write-Ahead Log (WAL)**: Crash recovery and durability
- **SSTables**: Persistent sorted data files on disk
- **Compaction**: Merges SSTables to optimize storage
- **Bloom Filters**: Fast negative lookups
- **Multi-level storage**: Automatic tiering of data by age

## Architecture

```
┌─────────────┐    flush     ┌──────────────┐
│  Memtable   │──────────────>│   Level 0    │
│ (SkipList)  │              │  (SSTables)  │
└─────────────┘              └──────────────┘
       │                            │
       │ WAL                        │ compaction
       v                            v
┌─────────────┐              ┌──────────────┐
│   wal.log   │              │   Level 1+   │
└─────────────┘              │  (SSTables)  │
                             └──────────────┘
```

## Usage

```go
db, err := NewEngine("./data")
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Write data
db.Set("key", "value")

// Read data
value, err := db.Get("key")

// Delete data
db.Delete("key")
```

## Project Structure

```
├── main.go           # Example usage
├── db.go             # Main database API
├── data/             # Generated data directory
│   └── manifest      # SSTable metadata
├── memtable/         # In-memory storage
│   ├── memtable.go
│   ├── skiplist.go
│   └── wal.go
├── sstable/          # Persistent storage
│   ├── reader.go
│   ├── writer.go
│   ├── compactor.go
│   ├── ssManager.go
│   ├── filter.go
│   └── format.go
└── shared/           # Common types
    ├── types.go
    └── format.go
```

## Running

```bash
go mod tidy
go run .
```

## What I Learned

- LSM trees optimize for write-heavy workloads
- Compaction is complex but essential for performance
- Trade-offs between write speed and read complexity
- How bloom filters reduce unnecessary disk reads
- WAL ensures durability in crash scenarios

## References

- [The Log-Structured Merge-Tree (LSM-Tree)](http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.44.2782)
- [Designing Data-Intensive Applications](https://dataintensive.net/)
