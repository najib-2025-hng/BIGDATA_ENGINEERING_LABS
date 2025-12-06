package edu.supmti.hadoop.hdfslab;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration; // [cite: 83]
import org.apache.hadoop.fs.*; // [cite: 84]

/**
 * Classe pour retourner l'état d'un fichier HDFS et le renommer.
 * Elle prend trois arguments en ligne de commande :
 * args[0]: Chemin du répertoire HDFS (ex: /user/root/input)
 * args[1]: Ancien nom du fichier (ex: purchases.txt)
 * args[2]: Nouveau nom du fichier (ex: achats.txt)
 */
public class HadoopFileStatus { // [cite: 89]

    public static void main(String[] args) { // [cite: 90]
        
        // Vérification du nombre d'arguments requis (3)
        if (args.length != 3) {
            System.err.println("Usage: hadoop jar <jarname> <repertoire_chemin> <ancien_nom_fichier> <nouveau_nom_fichier>");
            System.exit(1);
        }

        Configuration conf = new Configuration(); // [cite: 90]
        FileSystem fs = null;
        
        // Construction des chemins complets en utilisant les arguments
        Path dirPath = new Path(args[0]);
        Path oldFilePath = new Path(dirPath, args[1]);
        Path newFilePath = new Path(dirPath, args[2]);
        
        try {
            // Obtient l'instance du FileSystem
            fs = FileSystem.get(conf); // [cite: 92]

            // 1. Vérification de l'existence du fichier
            if (!fs.exists(oldFilePath)) {
                System.out.println("Erreur: Le fichier " + oldFilePath.toString() + " n'existe pas sur HDFS.");
                System.exit(1);
            }

            // 2. Obtenir et afficher les informations de statut du fichier
            FileStatus status = fs.getFileStatus(oldFilePath); // [cite: 93]
            
            System.out.println("----------------------------------------");
            System.out.println("STATUS DU FICHIER : " + status.getPath().getName());
            System.out.println("----------------------------------------");
            
            System.out.println("File Name: " + status.getPath().getName());
            System.out.println("File Size: " + Long.toString(status.getLen()) + " bytes"); // Utilise getLen() [cite: 98, 101]
            System.out.println("File owner: " + status.getOwner()); // [cite: 102]
            System.out.println("File permission: " + status.getPermission()); // [cite: 102]
            System.out.println("File Replication: " + status.getReplication()); // [cite: 102]
            System.out.println("File Block Size: " + status.getBlockSize()); // [cite: 102]

            // 3. Localisation des blocs
            BlockLocation[] blockLocations = fs.getFileBlockLocations(status, 0, status.getLen()); // [cite: 102]
            
            System.out.println("\nLOCALISATION DES BLOCS:");
            for (BlockLocation blockLocation : blockLocations) { // [cite: 103]
                System.out.println("  Block offset: " + blockLocation.getOffset()); // [cite: 106]
                System.out.println("  Block length: " + blockLocation.getLength()); // [cite: 106]
                
                String[] hosts = blockLocation.getHosts(); // [cite: 105]
                System.out.print("  Block hosts: ");
                for (String host : hosts) { // [cite: 108]
                    System.out.print(host + " "); // [cite: 110]
                }
                System.out.println(); // [cite: 111]
            }

            // 4. Renommage du fichier
            System.out.println("\n----------------------------------------");
            System.out.println("OPERATION DE RENOMMAGE");
            System.out.println("----------------------------------------");
            
            // Renomme le fichier: oldFilePath vers newFilePath
            if (fs.rename(oldFilePath, newFilePath)) { // Base sur l'opération de rename [cite: 112]
                System.out.println("Succès: Le fichier a été renommé de " + oldFilePath.getName() + " à " + newFilePath.getName());
            } else {
                System.out.println("Échec: Le renommage du fichier a échoué.");
            }

        } catch (IOException e) { // [cite: 113]
            System.err.println("Une erreur HDFS est survenue: " + e.getMessage());
            e.printStackTrace(); // [cite: 115]
        } finally {
            // Fermeture du FileSystem 
            if (fs != null) {
                try {
                    fs.close(); // [cite: 156]
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}