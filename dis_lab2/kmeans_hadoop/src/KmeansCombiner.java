package kmeans_hadoop.src;

import java.io.IOException;
import java.util.Iterator;

import javax.naming.Context;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import kmeans_hadoop.src.*;

public class KmeansCombiner extends Reducer<IntWritable, Record, IntWritable, Record> {

    public void reduce(IntWritable centroid, Iterable<Record> records, Context context)
            throws IOException, InterruptedException {

        Iterator<Record> it = records.iterator();
        if (!it.hasNext())
            return;

        // Start with the first record
        Record sum = Record.copy(it.next());

        // Sum remaining records
        while (it.hasNext()) {
            sum.sum(it.next());
        }

        context.write(centroid, sum);
    }
}
