package edu.supmti.hadoop.hdfslab;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

/**
 * Classe pour lire tout le contenu d'un fichier sur HDFS.
 * Elle prend un argument en ligne de commande :
 * args[0]: Chemin complet du fichier HDFS à lire (Ex: /user/root/input/achats.txt)
 */
public class ReadHDFS {

    public static void main(String[] args) {
        
        // Vérification de l'argument (un seul est requis : le chemin complet du fichier)
        if (args.length != 1) {
            System.err.println("Usage: hadoop jar <jarname> <chemin_complet_du_fichier_hdfs>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = null;
        FSDataInputStream inStream = null;
        BufferedReader br = null;
        
        // Le chemin du fichier est le premier argument
        Path filePath = new Path(args[0]);

        try {
            // Initialisation de la connexion HDFS
            fs = FileSystem.get(conf);
            
            // Vérification de l'existence avant de lire
            if (!fs.exists(filePath)) {
                System.err.println("Erreur: Le fichier " + filePath.toString() + " n'existe pas sur HDFS.");
                System.exit(1);
            }

            // Ouverture du flux de données HDFS pour la lecture (FSDataInputStream)
            inStream = fs.open(filePath);
            
            // Chaînage avec BufferedReader pour une lecture ligne par ligne optimisée
            InputStreamReader isr = new InputStreamReader(inStream);
            br = new BufferedReader(isr);
            
            String line;
            System.out.println("----------------------------------------");
            System.out.println("Contenu du fichier HDFS: " + filePath.getName());
            System.out.println("----------------------------------------");
            
            // Lecture et affichage ligne par ligne
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }

        } catch (IOException e) {
            System.err.println("Une erreur HDFS est survenue lors de la lecture: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Fermeture sécurisée des ressources
            try {
                if (br != null) br.close();
                if (inStream != null) inStream.close();
                if (fs != null) fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}