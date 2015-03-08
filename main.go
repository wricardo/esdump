package main

import (
	"log"
	"os"
	"strings"

	"github.com/codegangsta/cli"
	"github.com/wricardo/esdump/dumper"
)

func main() {
	app := cli.NewApp()
	app.Name = "esdump"
	app.Usage = "Dump your elastic search data"

	app.Commands = []cli.Command{
		{
			Name:  "dump",
			Usage: "dump your elastic search data",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "host",
					Value: "localhost",
					Usage: "Elastic search host",
				},
				cli.StringFlag{
					Name:  "port, p",
					Value: "9200",
					Usage: "Elastic search port",
				},
				cli.StringFlag{
					Name:  "scroll_size",
					Value: "1000",
					Usage: "Scroll size",
				},
				cli.StringFlag{
					Name:  "scroll_time",
					Value: "10m",
					Usage: "Scroll time",
				},
				cli.IntFlag{
					Name:  "concurrency, c",
					Value: 10,
					Usage: "Concurrency level",
				},
				cli.StringFlag{
					Name:  "index, i",
					Value: "",
					Usage: "indexes to be dumped. (coma separated)",
				},
				cli.StringFlag{
					Name:  "file, f",
					Value: "",
					Usage: "File where the data will be dumped to.",
				},
				cli.StringFlag{
					Name:  "directory, d",
					Value: "",
					Usage: "Diretory where the files will be dumped to.",
				},
				cli.StringFlag{
					Name:  "prefix",
					Value: "",
					Usage: "Prefix for the files when --directory",
				},
				cli.IntFlag{
					Name:  "chunk",
					Value: 10000,
					Usage: "How many entries per file",
				},
				cli.StringFlag{
					Name:  "format",
					Value: "raw_source",
					Usage: "Format in which the data will be presented. [raw_source|bulk_indexing]",
				},
			},
			Action: func(c *cli.Context) {
				d := &dumper.Dumper{
					EsHost:      c.String("host"),
					EsPort:      c.String("port"),
					Size:        c.String("scroll_size"),
					ScrollTime:  c.String("scroll_time"),
					Concurrency: c.Int("concurrency"),
				}

				d.Dest = destFactory(c)
				indexes := strings.Split(c.String("index"), ",")
				d.Dump(indexes)
				log.Println("Done")
			},
		},
	}
	app.Run(os.Args)
}

func destFactory(c *cli.Context) dumper.Destination {
	if c.String("directory") != "" {
		return &dumper.Folder{
			Dir:        c.String("directory"),
			FilePrefix: c.String("prefix"),
			FormatFunc: formatFuncFactory(c.String("format")),
			Chunks:     c.Int("chunk"),
		}
	} else if c.String("file") != "" {
		f, _ := os.Create(c.String("file"))
		return &dumper.File{
			File:       f,
			FormatFunc: formatFuncFactory(c.String("format")),
		}
	} else {
		return dumper.Stdout
	}
}
func formatFuncFactory(format string) dumper.FormatFunc {
	if format == "raw_source" {
		return dumper.RawSourceFormat
	} else if format == "bulk_indexing" {
		return dumper.BulkIndexingFormat
	} else {
		return dumper.RawSourceFormat
	}
}
