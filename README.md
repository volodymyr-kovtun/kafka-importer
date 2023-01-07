# kafka-importer
It's a simple console application that can download data from Kafka and save it to a specific folder (--destFolder argument).

### Filtration
There are the following filtration options
1. --topic. Import data from a specific topic [required]
2. --key. Import messages with a specific key [optional]

### Example

`dotnet run -- --bootstrap-servers "sport.kafka:9092" --topic  "sport-incidents" --key  "sport:4278966" --destFolder "kafka-messages"`