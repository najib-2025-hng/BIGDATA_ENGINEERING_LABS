package edu.supmti.hadoop.hdfslab;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

/**
 * Classe pour créer un nouveau fichier sur HDFS et y écrire du contenu.
 * Elle prend deux arguments en ligne de commande :
 * args[0]: Chemin complet du fichier HDFS à créer (ex: /user/root/output/nouveau_doc.txt)
 * args[1]: Contenu textuel additionnel à écrire dans le fichier
 */
public class HDFSWrite {

    public static void main(String[] args) {
        
        // Vérification des arguments (deux sont requis)
        if (args.length != 2) {
            System.err.println("Usage: hadoop jar <jarname> <chemin_complet_du_fichier_hdfs> <contenu_additionnel>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = null;
        FSDataOutputStream outStream = null;
        
        // Le chemin du nouveau fichier
        Path filePath = new Path(args[0]);
        // Le contenu additionnel
        String additionalContent = args[1];

        try {
            fs = FileSystem.get(conf);
            
            // 1. Création du fichier si celui-ci n'existe pas
            // Note: fs.create(filePath) écrase le fichier s'il existe par défaut, 
            // mais ce code vérifie l'existence avant d'opérer, comme c'est souvent le cas dans les TP.
            if (!fs.exists(filePath)) {

                System.out.println("Création du fichier HDFS : " + filePath.toString());
                
                // Crée le fichier et ouvre un flux d'écriture
                outStream = fs.create(filePath); 
                
                // 2. Écriture du contenu dans le flux
                outStream.writeUTF("--- Début du fichier HDFS. ---"); // Message par défaut
                outStream.writeUTF("Contenu passé en paramètre : " + additionalContent);
                
                System.out.println("Écriture réussie. Contenu additionnel : \"" + additionalContent + "\"");
                
            } else {
                System.out.println("Le fichier " + filePath.toString() + " existe déjà. Opération annulée.");
            }

        } catch (IOException e) {
            System.err.println("Une erreur HDFS est survenue lors de l'écriture: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 3. Fermeture du flux et du FileSystem pour s'assurer que les données sont écrites.
            try {
                if (outStream != null) outStream.close();
                if (fs != null) fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}