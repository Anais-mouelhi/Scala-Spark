import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SensorProducer {

    public static void main(String[] args) throws Exception {
        String[] cities = {"Paris", "Lyon", "Marseille", "Bordeaux", "Lille"};
        Random rand = new Random();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("key.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        System.out.println("Demarrage de l'envoi des donnees capteurs...");

        for (int i = 0; i < 50; i++) {
            String city        = cities[rand.nextInt(cities.length)];
            double temperature = 15 + rand.nextDouble() * 20; // 15-35 C
            double humidity    = 40 + rand.nextDouble() * 40; // 40-80%

            String message = String.format("%s,%.1f,%.1f", city, temperature, humidity);
            producer.send(new ProducerRecord<>("sensor-data", city, message));
            System.out.println("Envoye : " + message);
            Thread.sleep(300);
        }

        System.out.println("Tous les messages ont ete envoyes !");
        producer.close();
    }
}
