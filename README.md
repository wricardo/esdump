# esdump
```
$ ./esdump dump --help
NAME:
   dump - dump your elastic search data

USAGE:
   command dump [command options] [arguments...]

OPTIONS:
   --host "localhost"           Elastic search host
   --port, -p "9200"            Elastic search port
   --scroll_size "1000"         Scroll size
   --scroll_time "10m"          Scroll time
   --concurrency, -c "10"       Concurrency level
   --index, -i                  indexes to be dumped. (coma separated)
   --file, -f                   File where the data will be dumped to.
   --directory, -d              Diretory where the files will be dumped to.
   --prefix                     Prefix for the files when --directory
   --chunk "10000"              How many entries per file
   --format "raw_source"        Format in which the data will be presented. [raw_source|bulk_indexing]
```

Dumping two indexes to a directory:
```
./esdump dump --index some_index,another_index -d ./some_folder --format "bulk_indexing"
```
