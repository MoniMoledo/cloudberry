# Aedes Webview

## Build

Prerequisites: scala, sbt, [AsterixDB](http://asterixdb.apache.org), [Cloudberry](http://cloudberry.ics.uci.edu)

### Prepare the AsterixDB cluster
Follow the official [documentation](https://ci.apache.org/projects/asterixdb/install.html) to setup a fully functional cluster.

### Run Cloudberry service
You will need to give the AsterixDB cluster link to `neo` by change the `asterixdb.url` configuration in `neo/conf/application.conf` file.

The default value points to the localhost cluster
```
> cd cloudberry
> sbt "project neo" "run"
```

### Run Aedes Webview
Aedes Webview is an application that shows the Brazilian map with the mentions of Zika Virus, Dengue, Yellow Fever and Chikungunya diseases on twitter and news websites.

The datasets `webhose_ds.news` and `twitter.ds_tweet` on Asterix must be setup and have data. In order to crawl data to ingest on those datasets, follow the instructions on the project: [Crawler](https://github.com/MoniMoledo/webcrawler).

You can run the following command in a separate command line window:
```
> cd examples/aedeswebview
> sbt "project web" "run 9001"
```

### Tests

The following screenshots shows some examples of the first test made, with 1062 records on Asterix datasets.

Initial screen

![Initial screen](docs/aedeswebview/screen-empty.png?raw=true "Initial screen")

Dengue search on both datasets

![Dengue search on both datasets](docs/aedeswebview/screen-dengue.png?raw=true "Dengue search on both datasets")

Dengue search on city level

![Dengue search on city level](docs/aedeswebview/screen-city.png?raw=true "Dengue search on city level")


### Acknowledgments

* This application was inspired on the [TwitterMap Demo](http://cloudberry.ics.uci.edu/demos/twittermap/)
* Users and developers are welcome to contact me through moniquemoledo@id.uff.br