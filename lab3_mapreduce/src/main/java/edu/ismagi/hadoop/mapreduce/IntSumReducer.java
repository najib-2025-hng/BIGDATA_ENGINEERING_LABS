package edu.ismagi.hadoop.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.lang.Iterable;

public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        
        int sum = 0;
        
        // Sommation de toutes les valeurs (les '1')
        for (IntWritable val : values) {
            sum += val.get();
        }
        
        result.set(sum);
        // Écriture du résultat final (mot, total_occurrences)
        context.write(key, result);
    }
}