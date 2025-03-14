package kmeans_hadoop.src;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KmeansDriver {

    private static boolean stoppingCriterion(Record[] oldCentroids, Record[] newCentroids, double threshold) {
        for (int i = 0; i < oldCentroids.length; i++) {
            if (oldCentroids[i].distance(newCentroids[i]) > threshold) {
                return false;
            }
        }
        return true;
    }

    private static Record[] centroidsInit(Configuration conf, String inputPath, int k, int dataSetSize)
            throws IOException {
        List<Integer> positions = new ArrayList<>();
        Random random = new Random();

        while (positions.size() < k) {
            int pos = random.nextInt(dataSetSize);
            if (!positions.contains(pos)) {
                positions.add(pos);
            }
        }

        Collections.sort(positions);
        Path path = new Path(inputPath);
        FileSystem fs = FileSystem.get(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));

        Record[] centroids = new Record[k];
        int lineNo = 0, idx = 0;
        String line;
        while ((line = br.readLine()) != null && idx < k) {
            if (lineNo == positions.get(idx)) {
                centroids[idx] = new Record(line.split(","), new int[] { lineNo });
                idx++;
            }
            lineNo++;
        }

        br.close();
        return centroids;
    }

    private static Map<Integer, Record> readClustersFromHDFS(Configuration conf, int k, String pathStr)
            throws IOException {
        Map<Integer, Record> clusterMap = new HashMap<>();
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(new Path(pathStr));

        for (FileStatus file : status) {
            Path path = file.getPath();
            if (path.getName().startsWith("part")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
                String line;
                while ((line = br.readLine()) != null) {
                    String[] kv = line.split("\t");
                    int clusterId = Integer.parseInt(kv[0]);
                    Record centroid = new Record(kv[1].split(","), new int[] {});
                    clusterMap.put(clusterId, centroid);
                }
                br.close();
            }
        }

        fs.delete(new Path(pathStr), true); // delete intermediate
        return clusterMap;
    }

    private static void writeFinalClusters(Configuration conf, Map<Integer, Record> clusters, String outputPath)
            throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path output = new Path(outputPath + "/clusters.txt");
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(output, true)));

        for (Map.Entry<Integer, Record> entry : clusters.entrySet()) {
            Record centroid = entry.getValue();
            br.write(centroid.getFeaturesString() + "\n");
            // br.write("Centroid: " + centroid.getFeaturesString() + "\n");
            // br.write("Indexes: " + centroid.getIndexesString() + "\n\n");
        }

        br.close();
    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();

        Configuration conf = new Configuration();
        conf.addResource(new Path("config.xml"));

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: <input> <output>");
            System.exit(1);
        }

        String input = otherArgs[0];
        String output = otherArgs[1] + "/temp";
        int datasetSize = conf.getInt("dataset", 150);
        int k = conf.getInt("k", 4);
        double threshold = conf.getDouble("threshold", 0.0001);
        int maxIter = conf.getInt("max.iteration", 1);

        Record[] oldCentroids = new Record[k];
        Record[] newCentroids = centroidsInit(conf, input, k, datasetSize);
        for (int i = 0; i < k; i++) {
            conf.set("centroid." + i, newCentroids[i].getFeaturesString());
        }

        boolean converged = false;
        int iteration = 0;

        while (!converged && iteration < maxIter) {
            iteration++;

            Job job = Job.getInstance(conf, "KMeans Iteration " + iteration);
            job.setJarByClass(KmeansDriver.class);
            job.setMapperClass(KmeansMapper.class);
            job.setCombinerClass(KmeansCombiner.class);
            job.setReducerClass(KmeansReducer.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Record.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Record.class);
            job.setNumReduceTasks(k);
            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output));
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            if (!job.waitForCompletion(true)) {
                System.err.println("Iteration " + iteration + " failed.");
                System.exit(1);
            }

            for (int i = 0; i < k; i++) {
                oldCentroids[i] = Record.copy(newCentroids[i]);
            }

            Map<Integer, Record> clusterMap = readClustersFromHDFS(conf, k, output);
            for (int i = 0; i < k; i++) {
                newCentroids[i] = clusterMap.get(i);
            }

            converged = stoppingCriterion(oldCentroids, newCentroids, threshold);

            if (!converged) {
                for (int i = 0; i < k; i++) {
                    conf.unset("centroid." + i);
                    conf.set("centroid." + i, newCentroids[i].getFeaturesString());
                }
            } else {
                writeFinalClusters(conf, clusterMap, otherArgs[1]);
            }
        }

        long end = System.currentTimeMillis();
        System.out.println("Execution Time: " + (end - start) + " ms");
        System.out.println("Iterations: " + iteration);
        System.exit(0);
    }
}
