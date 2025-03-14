package kmeans_hadoop.src;

import java.io.IOException;
import java.util.Iterator;


import javax.naming.Context;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import kmeans_hadoop.src.*;

public class KmeansReducer extends Reducer<IntWritable, Record, Text, Text> {

    private final Text centroidId = new Text();
    private final Text centroidValue = new Text();

    public void reduce(IntWritable centroid, Iterable<Record> partialSums, Context context)
            throws IOException, InterruptedException {

        // Sum the partial sums
        Iterator<Record> it = partialSums.iterator();
        if (!it.hasNext())
            return;

        Record sum = Record.copy(it.next());

        while (it.hasNext()) {
            sum.sum(it.next());
        }

        // Calculate the new centroid
        sum.average();

        centroidId.set(centroid.toString());
        centroidValue.set(sum.toString());
        context.write(centroidId, centroidValue);
    }
}
