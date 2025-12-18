package edu.supmti;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import java.util.Arrays;
import java.util.Properties;
// L'import pour 'Printed' est supprimé car il causait des erreurs d'incompatibilité de version.

public class WordCountConsumer {

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.err.println("Usage: java -jar <votre_jar>.jar WordCountConsumer <topic_entree>");
            System.exit(1);
        }

        final String INPUT_TOPIC = args[0];
        final String OUTPUT_TOPIC = "WordCount-Output"; // Topic où les résultats seront écrits

        // 1. Configuration des propriétés de Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        // Configuration pour la gestion des données (nécessaire pour KTable)
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);

        // 2. Construction de la Topologie Kafka Streams
        StreamsBuilder builder = new StreamsBuilder();
        // Lire le Topic d'entrée (String, String)
        KStream<String, String> textLines = builder.stream(INPUT_TOPIC);

        // 3. Logique Word Count :
        KTable<String, Long> wordCounts = textLines
                // 1. Mettre le texte en minuscules et séparer par espace
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                // 2. Mettre le mot (value) comme Clé pour le comptage
                .selectKey((key, word) -> word)
                // 3. Grouper par la nouvelle Clé (le mot)
                .groupByKey()
                // 4. Compter les occurrences
                .count();

        // 4. Écrire le résultat dans un Topic de sortie
        // Convertit le KTable (état agrégé) en KStream et l'envoie vers le Topic 'WordCount-Output'.
        // Les clés sont des Strings (le mot) et les valeurs sont des Longs (le compte).
        wordCounts.toStream().to(
                OUTPUT_TOPIC,
                Produced.with(Serdes.String(), Serdes.Long()) // Sortie : String, Long
        );
        
        // L'affichage en console est supprimé ici pour éviter les erreurs de compatibilité Java/Maven.
        
        // 5. Démarrer l'application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp(); // Nettoyage de l'état précédent (utile lors des tests)
        streams.start();

        // Ajouter un shutdown hook pour arrêter l'application proprement
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}