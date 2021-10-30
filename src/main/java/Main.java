import org.apache.kafka.clients.producer.*;
import java.util.Properties;
import java.util.Random;
import java.io.IOException;
import java.nio.file.*;


public class Main {
    static final  int PARTITION_COUNT = 6;
    static final String[] city_names_full = new String[]{"Aberdeen", "Abilene", "Akron", "Albany", "Albuquerque",
            "Alexandria", "Allentown", "Amarillo", "Anaheim", "Anchorage", "Ann Arbor", "Antioch", "Apple Valley",
            "Appleton", "Arlington", "Arvada", "Asheville", "Athens", "Atlanta", "Atlantic City", "Augusta",
            "Aurora", "Austin", "Bakersfield", "Baltimore", "Barnstable", "Baton Rouge", "Beaumont", "Bel Air",
            "Bellevue", "Berkeley", "Bethlehem", "Billings", "Birmingham", "Bloomington", "Boise", "Boise City",
            "Bonita Springs", "Boston", "Boulder", "Bradenton", "Bremerton", "Bridgeport", "Brighton",
            "Brownsville", "Bryan", "Buffalo", "Burbank", "Burlington", "Cambridge", "Canton", "Cape Coral",
            "Carrollton", "Cary", "Cathedral City", "Cedar Rapids", "Champaign", "Chandler", "Charleston",
            "Charlotte", "Chattanooga", "Chesapeake", "Chicago", "Chula Vista", "Cincinnati", "Clarke County",
            "Clarksville", "Clearwater", "Cleveland", "College Station", "Colorado Springs", "Columbia", "Columbus",
            "Concord", "Coral Springs", "Corona", "Corpus Christi", "Costa Mesa", "Dallas", "Daly City", "Danbury",
            "Davenport", "Davidson County", "Dayton", "Daytona Beach", "Deltona", "Denton", "Denver", "Des Moines",
            "Detroit", "Downey", "Duluth", "Durham", "El Monte", "El Paso", "Elizabeth", "Elk Grove", "Elkhart",
            "Erie", "Escondido", "Eugene", "Evansville", "Fairfield", "Fargo", "Fayetteville", "Fitchburg", "Flint",
            "Fontana", "Fort Collins", "Fort Lauderdale", "Fort Smith", "Fort Walton Beach", "Fort Wayne",
            "Fort Worth", "Frederick", "Fremont", "Fresno", "Fullerton", "Gainesville", "Garden Grove", "Garland",
            "Gastonia", "Gilbert", "Glendale", "Grand Prairie", "Grand Rapids", "Grayslake", "Green Bay",
            "GreenBay", "Greensboro", "Greenville", "Gulfport-Biloxi", "Hagerstown", "Hampton", "Harlingen",
            "Harrisburg", "Hartford", "Havre de Grace", "Hayward", "Hemet", "Henderson", "Hesperia", "Hialeah",
            "Hickory", "High Point", "Hollywood", "Honolulu", "Houma", "Houston", "Howell", "Huntington",
            "Huntington Beach", "Huntsville", "Independence", "Indianapolis", "Inglewood", "Irvine", "Irving",
            "Jackson", "Jacksonville", "Jefferson", "Jersey City", "Johnson City", "Joliet", "Kailua", "Kalamazoo",
            "Kaneohe", "Kansas City", "Kennewick", "Kenosha", "Killeen", "Kissimmee", "Knoxville", "Lacey",
            "Lafayette", "Lake Charles", "Lakeland", "Lakewood", "Lancaster", "Lansing", "Laredo", "Las Cruces",
            "Las Vegas", "Layton", "Leominster", "Lewisville", "Lexington", "Lincoln", "Little Rock", "Long Beach",
            "Lorain", "Los Angeles", "Louisville", "Lowell", "Lubbock", "Macon", "Madison", "Manchester", "Marina",
            "Marysville", "McAllen", "McHenry", "Medford", "Melbourne", "Memphis", "Merced", "Mesa", "Mesquite",
            "Miami", "Milwaukee", "Minneapolis", "Miramar", "Mission Viejo", "Mobile", "Modesto", "Monroe",
            "Monterey", "Montgomery", "Moreno Valley", "Murfreesboro", "Murrieta", "Muskegon", "Myrtle Beach",
            "Naperville", "Naples", "Nashua", "Nashville", "New Bedford", "New Haven", "New London", "New Orleans",
            "New York", "New York City", "Newark", "Newburgh", "Newport News", "Norfolk", "Normal", "Norman",
            "North Charleston", "North Las Vegas", "North Port", "Norwalk", "Norwich", "Oakland", "Ocala",
            "Oceanside", "Odessa", "Ogden", "Oklahoma City", "Olathe", "Olympia", "Omaha", "Ontario", "Orange",
            "Orem", "Orlando", "Overland Park", "Oxnard", "Palm Bay", "Palm Springs", "Palmdale", "Panama City",
            "Pasadena", "Paterson", "Pembroke Pines", "Pensacola", "Peoria", "Philadelphia", "Phoenix",
            "Pittsburgh", "Plano", "Pomona", "Pompano Beach", "Port Arthur", "Port Orange", "Port Saint Lucie",
            "Port St. Lucie", "Portland", "Portsmouth", "Poughkeepsie", "Providence", "Provo", "Pueblo",
            "Punta Gorda", "Racine", "Raleigh", "Rancho Cucamonga", "Reading", "Redding", "Reno", "Richland",
            "Richmond", "Richmond County", "Riverside", "Roanoke", "Rochester", "Rockford", "Roseville",
            "Round Lake Beach", "Sacramento", "Saginaw", "Saint Louis", "Saint Paul", "Saint Petersburg", "Salem",
            "Salinas", "Salt Lake City", "San Antonio", "San Bernardino", "San Buenaventura", "San Diego",
            "San Francisco", "San Jose", "Santa Ana", "Santa Barbara", "Santa Clara", "Santa Clarita", "Santa Cruz",
            "Santa Maria", "Santa Rosa", "Sarasota", "Savannah", "Scottsdale", "Scranton", "Seaside", "Seattle",
            "Sebastian", "Shreveport", "Simi Valley", "Sioux City", "Sioux Falls", "South Bend", "South Lyon",
            "Spartanburg", "Spokane", "Springdale", "Springfield", "St. Louis", "St. Paul", "St. Petersburg",
            "Stamford", "Sterling Heights", "Stockton", "Sunnyvale", "Syracuse", "Tacoma", "Tallahassee", "Tampa",
            "Temecula", "Tempe", "Thornton", "Thousand Oaks", "Toledo", "Topeka", "Torrance", "Trenton", "Tucson",
            "Tulsa", "Tuscaloosa", "Tyler", "Utica", "Vallejo", "Vancouver", "Vero Beach", "Victorville",
            "Virginia Beach", "Visalia", "Waco", "Warren", "Washington", "Waterbury", "Waterloo", "West Covina",
            "West Valley City", "Westminster", "Wichita", "Wilmington", "Winston", "Winter Haven", "Worcester",
            "Yakima", "Yonkers", "York", "Youngstown"};

    static final String[] city_names = new String[]{"Baltimore", "Barnstable", "Baton Rouge", "Beaumont", "Bel Air"};

    public static void main(String[] args) {
        int arguments = args.length;
        if(arguments == 0){
            System.out.println("Running Default benchmark: 10.000.000, 50, 10.000, 5000");
        failureBenchmark(10_000_000, 50, 10_000, 5000);
        } else if(args[0].equals("-benchmark") && arguments==5){
            failureBenchmark(Long.parseLong(args[1]), Long.parseLong(args[2]), Long.parseLong(args[3]), Long.parseLong(args[4]));
        } else if(args[0].equals("-send") && arguments==5){
            send_n_messages(Long.parseLong(args[1]), Long.parseLong(args[2]), Long.parseLong(args[3]), Long.parseLong(args[4]));
        } else{
            System.out.println("Usage: -benchmark | - send  messag_count sleep_short sleep_long sleep_factor");
        }

    }

    public static void send_n_messages(long msg_count, long sleep_short, long sleep_long, long sleep_factor){
        Properties props = new Properties();
        // Docker container id/IP if not running from docker container in the overlay
        // network (here running it on the kafka container itself -> localhost )
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 10);
        //props.put("linger.ms", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        int partition = 0;
        for (int i = 0; i < msg_count + PARTITION_COUNT; i++) {
            if (i % sleep_factor == 0) {
                sleep(sleep_short);
            }
            if ( i == msg_count ) {
                sleep(sleep_long);
            }
            // partition 1 should on avg be 2 x partition 0
            long data = (long) (Math.random() * (100)) * (partition + 1);
            Random r = new Random();
            String city = city_names[r.nextInt(city_names.length)];
            long localTimestamp = System.currentTimeMillis(); // .nanoTime(); maybe not good to compare as we
            // investigate everything else in millis
            KafkaCallback callback = new KafkaCallback(i, partition, city, data, localTimestamp);
            producer.send(new ProducerRecord<String, String>("mytopic", partition, (Integer.toString(i)),
                            "{\"venue\":{\"country\": \"US\", \"city\": \"" + city + "\" }, \"sensordata\":\"" + data + "\"}"),
                    callback);
            partition = (partition + 1) % PARTITION_COUNT;
        }
        producer.close();

        String line = "msg_count: " + msg_count + "; sleep_short: "+ sleep_short + "; sleep_long: " + sleep_long + "; sleep_factor: " + sleep_factor + "\n";
        try {
            Files.write(Paths.get("produceroutput.csv"), line.getBytes(), StandardOpenOption.APPEND);
        } catch (IOException e) {
        }

        sleep(60000);
    }

    public static void throughputTest(){
        send_n_messages(500000 * 20,50, 10000, 8500 );
        send_n_messages(500000 * 40, 50, 10000, 20500 ); 
        send_n_messages(500000 * 40, 50, 10000, 22500 ); 
        send_n_messages(500000 * 40, 50, 10000, 24500 ); 
        send_n_messages(500000 * 40, 50, 10000, 26500 ); 
        send_n_messages(500000 * 40, 50, 10000, 28500 ); 
        send_n_messages(500000 * 40, 50, 10000, 29500 ); 
        send_n_messages(500000 * 40, 50, 10000, 30500 ); //all working now, but seems like there is backpressure (increasing latencies)
    }

    public static void failureBenchmark(long msg_count, long sleep_short, long sleep_long, long sleep_factor){
        //warmup
        send_n_messages(1_000_000 * 5, 50, 10000, 5000);
        //first run
        send_n_messages(msg_count, sleep_short, sleep_long, sleep_factor);
        //second run
        send_n_messages(msg_count, sleep_short, sleep_long, sleep_factor);
    }
    public static void sleep(long millies){
        try{
            Thread.sleep(millies);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void send_parallel(int thread_count){
        for(int i=0; i < thread_count; i++){
            Thread thread = new Thread(new MultiThreadProducer());
            thread.start();
        }
    }
}
// The result of the send is a RecordMetadata specifying the partition the
// record was sent to, the offset it was assigned and the timestamp of the
// record. If CreateTime is used by the topic, the timestamp will be the user
// provided timestamp or the record send time if the user did not specify a
// timestamp for the record. If LogAppendTime is used for the topic, the
// timestamp will be the Kafka broker local time when the message is appended.
// https://kafka.apache.org/28/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html