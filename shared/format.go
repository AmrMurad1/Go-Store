package shared

const (
	MagicNumber uint64 = 0xDEADBEEFCAFE
	FooterSize  int    = 44
)

type IndexRecord struct {
	LastKey Key
	Offset  int64
	Size    int32
}

type MetaBlock struct {
	EntryCount uint64
	MinKey     Key
	MaxKey     Key
	Timestamp  int64
}

type Footer struct {
	MetaBlockOffset  int64
	MetaBlockSize    uint32
	IndexBlockOffset int64
	IndexBlockSize   uint32
	FilterOffset     int64
	FilterSize       uint32
	Magic            uint64
}
