package kmeans_hadoop.src;

import java.io.IOException;

import javax.naming.Context;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import kmeans_hadoop.src.*;

/*
 * Simply here I will get one point and list of centroids
 * All my task to assign this point to the nearest centroid
 * KEYIN: number of the line in the data file
 * VALUEIN: the exact feature of the record
 * KEYOUT: centroid ID [0, k[
 * VALUEOUT: the exact features of the record
 */

public class KmeansMapper extends Mapper<LongWritable, Text, IntWritable, Record> {

    private Record[] centroids;
    private final Record Record = new Record();
    private final IntWritable centroid = new IntWritable();

    public void setup(Context context) {
        int k = Integer.parseInt(context.getConfiguration().get("k"));

        this.centroids = new Record[k];
        for (int i = 0; i < k; i++) {
            String centroidStr = context.getConfiguration().get("centroid." + i);
            this.centroids[i] = new Record(centroidStr.split(","), new int[]{-1});
        }
    }

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Contruct the Record
        String[] RecordString = value.toString().split(",");
        int index = (int) key.get();
        Record.set(RecordString, new int[]{index});

        // Initialize variables
        Double minDist = Double.POSITIVE_INFINITY;
        Double distance = 0.0;
        int nearest = -1;

        // Find the closest centroid
        for (int i = 0; i < centroids.length; i++) {
            distance = Record.distance(centroids[i]);
            if (distance < minDist) {
                nearest = i;
                minDist = distance;
            }
        }

        centroid.set(nearest);
        context.write(centroid, Record);
    }
}
