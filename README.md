# kafka-generator
Meant to generate simple testdata for example topology in: [jfr212/storm-topology](https://github.com/jfr2102/storm-topology)
Included in kafka image in [jfr212/storm-cluster(https://github.com/jfr2102/storm-cluster) 

### configuration:
- Messages are sent in round robin to partition 0 and 1 of topic "mytopic". Simply edit .send() methods parms accordingly
- Format: JSON style String: e.g: {"venue": {"country": "US", "city": "New York"}, "sensordata": "100"}
### build:
```bash
mvn assembly:assembly
```
### run: 
```bash
java -jar jarname-with-dependencies.jar
```
