package edu.supmti;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;
import java.io.IOException;

public class WordProducer {

    public static void main(String[] args) {

        // Vérification que le nom du Topic est fourni en argument
        if (args.length != 1) {
            System.err.println("Usage: java -jar <votre_jar>.jar WordProducer <nom_du_topic>");
            System.err.println("Exemple: java -jar ... WordProducer WordCount-Topic");
            System.exit(1);
        }

        final String TOPIC = args[0]; // Récupération du nom du Topic depuis l'argument

        // 1. Configuration des propriétés du Producteur
        Properties props = new Properties();
        
        // CORRECTION CRITIQUE : Adresse du Broker Kafka (doit être localhost:9092 dans le conteneur)
        props.put("bootstrap.servers", "localhost:9092"); 
        
        // VÉRIFICATION CRITIQUE : Sérialisation des Clés et des Valeurs
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        // Configuration de la durabilité (optionnel mais recommandé)
        props.put("acks", "all");

        // 2. Création du Producteur et Logique d'envoi
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            
            System.out.println("--- Producteur WordProducer démarré ---");
            System.out.println("Topic cible : " + TOPIC);
            System.out.println("Veuillez taper votre texte (tapez 'exit' pour quitter).");
            
            // Lecture de l'entrée utilisateur depuis la console
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            String line;

            while (true) {
                System.out.print("> ");
                line = reader.readLine();

                if (line == null || line.equalsIgnoreCase("exit")) {
                    break; 
                }
                
                // Créer l'enregistrement : la valeur est la ligne tapée
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, null, line);
                
                // Envoyer le message
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Erreur lors de l'envoi au topic " + TOPIC + ": " + exception.getMessage());
                    }
                });
                
                // S'assurer que le message est immédiatement envoyé
                producer.flush();
            }

        } catch (IOException e) {
            System.err.println("Erreur de lecture de l'entrée utilisateur : " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Erreur fatale du Producer : " + e.getMessage());
            e.printStackTrace();
        }
    }
}