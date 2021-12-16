# kafka-generator
Meant to generate simple test data for example topology in: [jfr212/storm-topology](https://github.com/jfr2102/storm-topology)
Included in kafka image in [jfr212/storm-cluster](https://github.com/jfr2102/storm-cluster) 

## Configuration:
- Messages are sent in round robin to partitions of "mytopic" Kafka topic. Simply edit .send() methods parms accordingly
- Format: JSON style String: e.g: {"venue": {"country": "US", "city": "New York"}, "sensordata": "100"}
## Build:
```bash
mvn assembly:assembly
```
## Run:
```bash
java -jar jarname-with-dependencies.jar
```
### Arguments: 
#### -benchmark *message_count* *sleep_short* *sleep_long* *microbatch_size*:
  
  Sends 2*message_count messages in 2 intervals.
  
  Microbatches of microbatch_size are generated with sleeping time of *sleep_short* ms in between.
  
  Sleep *sleep_long* ms before last message of each partition within the 2 sending intervals (useful for event-time watermarks in SPS)

#### -throughput 
  Send messages in intervals with increasing throughput (e.g. to benchmark sustainable throughput of SPS)
