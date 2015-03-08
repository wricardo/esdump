package dumper

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/jeffail/gabs"
)

// Defaults
const (
	es_host     = "localhost"
	es_port     = "9200"
	size        = "500"
	scrollTime  = "10m"
	concurrency = 1
)

var (
	defaultDest = Stdout
)

type Dumper struct {
	EsHost      string
	EsPort      string
	Size        string
	ScrollTime  string
	Concurrency int
	Dest        Destination
	FormatFunc  FormatFunc
}

func New() *Dumper {
	return &Dumper{
		EsHost:      es_host,
		EsPort:      es_port,
		Size:        size,
		ScrollTime:  scrollTime,
		Concurrency: concurrency,
		Dest:        defaultDest,
	}
}

//Dump fetches elastic search for the indexes data and writes the data to Dest
func (d *Dumper) Dump(indexes []string) {
	runtime.GOMAXPROCS(8)
	sc := make(chan scroll, d.Concurrency)

	indexDumped := sync.WaitGroup{}

	for i := 0; i < d.Concurrency; i++ {
		go d.processScrolls(sc, &indexDumped)
	}

	indexDumped.Add(len(indexes))
	for _, index := range indexes {
		scrollId := d.createScroll(index)
		sc <- scroll{
			index:    index,
			scrollId: scrollId,
		}
	}
	indexDumped.Wait()
	close(sc)
}

// createScroll creates the scroll from the index
func (d *Dumper) createScroll(index string) string {
	content, err := d.esGet("http://" + es_host + ":" + es_port + "/" + index + "/_search?scroll=" + scrollTime + "&size=" + size + "&search_type=scan")
	if err != nil {
		log.Fatal(err)
	}

	sr := new(ScrollResponse)
	err = json.Unmarshal(content, sr)
	if err != nil {
		log.Fatal(err)
	}
	return sr.ScrollId
}

// processScrolls processes the scroll until there is no more data on the scroll.
// It calls done on the WaitGroup if we have fetched all the data on the index
func (d *Dumper) processScrolls(sc chan scroll, indexDumped *sync.WaitGroup) {
	for v := range sc {
		ns := d.processScroll(v)
		if ns != nil {
			select {
			case sc <- *ns:
			case <-time.After(time.Second * 5):
				fmt.Println("timout")
			}

		} else {
			indexDumped.Done()
		}
	}
}

// processScroll fetches the data and sends it to the DocumentProcessor.
// It returns a new *scroll if there is more data to fetch
func (d *Dumper) processScroll(v scroll) *scroll {
	content, err := d.esGet(d.scrollUrl(v.scrollId))
	if err != nil {
		log.Fatal(err)
	}

	hits, newScrollId, err := d.getHitsAndNewScrollId(content)
	if err != nil {
		log.Fatal(err)
	}

	if len(hits) > 0 {
		for _, hit := range hits {
			d.Dest.Process(documentFromHit(hit))
		}
		return &scroll{
			index:    v.index,
			scrollId: newScrollId,
		}
	} else {
		return nil
	}
}

// getHitsAndNewScrollId parses the content and returns a slice of hits from elastic search and a new _scroll_id
func (d *Dumper) getHitsAndNewScrollId(content []byte) ([]*gabs.Container, string, error) {
	jsonParsedObj, err := gabs.ParseJSON(content)
	if err != nil {
		return nil, "", err
	}

	hits, err := jsonParsedObj.Search("hits").Search("hits").Children()
	if err != nil {
		return nil, "", err
	}
	newScrollId := jsonParsedObj.Path("_scroll_id").Data().(string)
	return hits, newScrollId, nil
}

// scrollUrl returns a url that is used to get data from a scrollId
func (d *Dumper) scrollUrl(scrollId string) string {
	return "http://" + es_host + ":" + es_port + "/_search/scroll?scroll=" + scrollTime + "&search_type=scan&size=" + size + "&scroll_id=" + scrollId
}

// esGet performs a get request againt an url and returns the response.
func (d *Dumper) esGet(url string) ([]byte, error) {
	r, err := http.Get(url)
	defer r.Body.Close()

	if err != nil {
		return nil, err
	}

	content, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	return content, nil
}

type ScrollResponse struct {
	ScrollId string `json:"_scroll_id"`
	Hits     struct {
		Total int `json:'total'`
	} `json:"hits"`
}

type scroll struct {
	index    string
	scrollId string
}

func documentFromHit(hit *gabs.Container) *Document {
	return &Document{
		Index:  hit.Path("_index").Data().(string),
		Type:   hit.Path("_type").Data().(string),
		Id:     hit.Path("_id").Data().(string),
		source: []byte(hit.Path("_source").String()),
	}
}
