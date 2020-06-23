package datastore

type Offset interface {
	Offset() int64
	Metadata() string
}

type offsetType struct {
	OffsetV   int64  `json:"offset"`
	MetadataV string `json:"metadata"`
}

func (o *offsetType) Offset() int64 {
	return o.OffsetV
}

func (o *offsetType) Metadata() string {
	return o.MetadataV
}

func NewOffset(offset int64, metadata string) Offset {
	return &offsetType{
		OffsetV:   offset,
		MetadataV: metadata,
	}
}
