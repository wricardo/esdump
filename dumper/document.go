package dumper

import "fmt"

// Document represents a document on elastic search
type Document struct {
	Index  string `json:"_index"`
	Type   string `json:"_type"`
	Id     string `json:"_id"`
	source []byte
}

// Source returns a json encoded version of the data of the document
func (d *Document) Source() []byte {
	return d.source
}

// BulkIndexingFormat returns the data from the document in the format used for indexing in bulk
func (d *Document) BulkIndexingFormat() []byte {
	return BulkIndexingFormat(d)
}

// Format func is a function that takes a document and returns a []byte representation of it
type FormatFunc func(d *Document) []byte

// Source returns a json encoded version of the data of the document
func RawSourceFormat(d *Document) []byte {
	return append(d.source, '\n')
}

// BulkIndexingFormat returns the data from the document in the format used for indexing in bulk
func BulkIndexingFormat(d *Document) []byte {
	b := make([]byte, 0)
	b = append(b, []byte(fmt.Sprintf("{\"index\":{\"_index\":\"%v\",\"_type\":\"%v\",\"_id\":\"%v\"}}\n", d.Index, d.Type, d.Id))...)
	b = append(b, d.Source()...)
	b = append(b, '\n')
	return b
}
