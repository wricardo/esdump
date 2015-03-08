package dumper

import (
	"io"
	"io/ioutil"
	"os"
	"sync"
)

var (
	Stdout = &Writer{
		Writer:     os.Stdout,
		FormatFunc: RawSourceFormat,
	}
	StdoutBulkIndexing = &Writer{
		Writer:     os.Stdout,
		FormatFunc: BulkIndexingFormat,
	}
	CurrentFolder = &Folder{
		Dir:        "./",
		FormatFunc: RawSourceFormat,
	}
	CurrentFolderBulkIndexing = &Folder{
		Dir:        "./",
		FormatFunc: BulkIndexingFormat,
	}
)

// DocumentProcessor is an interface that defines how to process a document
type Destination interface {
	Process(*Document)
}

// Folder implements DocumentProcessor and it writes documents to the disk based on the index of the document
type Folder struct {
	sync.RWMutex
	Dir           string
	FilePrefix    string
	FormatFunc    FormatFunc
	Chunks        int
	fileWriterMap map[string]File
	counterMap    map[string]int
}

func (f *Folder) incrCounter(index string) bool {
	f.RLock()
	if f.Chunks == 0 {
		f.RUnlock()
		return false
	}
	_, exists := f.counterMap[index]
	f.RUnlock()
	if exists == false {
		f.Lock()
		f.counterMap = make(map[string]int)
		f.Unlock()
	}
	f.Lock()
	f.counterMap[index] = f.counterMap[index] + 1
	defer f.Unlock()
	if f.counterMap[index] >= f.Chunks {
		f.counterMap[index] = 0
		return true
	}
	return false
}

// returns a "singleton" fileWriter based on the index
func (f *Folder) fileWriter(index string) File {
	f.RLock()
	fwm := f.fileWriterMap
	f.RUnlock()

	if fwm == nil {
		f.Lock()
		f.fileWriterMap = make(map[string]File)
		fwm = f.fileWriterMap
		f.Unlock()
	}
	f.RLock()
	_, exists := fwm[index]
	f.RUnlock()

	if exists == false {
		f.newFileWriter(index)
		fwm = f.fileWriterMap
	}
	f.RLock()
	defer f.RUnlock()
	return fwm[index]
}

func (f *Folder) newFileWriter(index string) {
	f.Lock()
	file, _ := ioutil.TempFile(f.Dir, f.FilePrefix+index+"-")
	f.fileWriterMap[index] = File{
		File:       file,
		FormatFunc: f.FormatFunc,
	}
	f.Unlock()
}

// Process writes documents to the disk based on the index of the document
func (f *Folder) Process(d *Document) {
	if f.incrCounter(d.Index) == true {
		f.newFileWriter(d.Index)
	}
	fw := f.fileWriter(d.Index)
	fw.Process(d)
}

func (f *Folder) Close() {
	f.Lock()
	for _, v := range f.fileWriterMap {
		v.Close()
	}
	f.Unlock()
}

type File struct {
	File       *os.File
	FormatFunc FormatFunc
}

func (fw File) Process(d *Document) {
	fw.File.Write(fw.FormatFunc(d))
}

func (fw File) Close() {
	fw.File.Close()
}

type Writer struct {
	Writer     io.Writer
	FormatFunc FormatFunc
}

func (d Writer) Process(doc *Document) {
	d.Writer.Write(d.FormatFunc(doc))
}
