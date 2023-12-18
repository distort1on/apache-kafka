# apache-kafka
Top 10 tx by price from bitstamp in real-time

## Requirements

* [Docker 24+](https://www.docker.com/get-started)
* [GO 21+](https://go.dev/doc/install)

## Bootstraping your environment

```sh
# to run kafka locally
$ docker-compose up -d
# wait while environment initialization is complete
$ make all
```

## Run
```sh
# run in different terminal windows
$ ./producer
$ ./consumer
```
