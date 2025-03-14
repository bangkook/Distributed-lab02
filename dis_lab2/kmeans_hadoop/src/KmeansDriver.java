package kmeans_hadoop.src;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class KmeansDriver {

    private static final String CENTROID_PREFIX = "centroid.";

    private static boolean hasConverged(Record[] oldCentroids, Record[] newCentroids, double threshold) {
        for (int i = 0; i < oldCentroids.length; i++) {
            if (oldCentroids[i].distance(newCentroids[i]) > threshold) {
                return false;
            }
        }
        return true;
    }

    private static void updateCentroidConfig(Configuration conf, Record[] centroids) {
        for (int i = 0; i < centroids.length; i++) {
            conf.set(CENTROID_PREFIX + i, centroids[i].toString());
        }
    }

    private static Record[] initCentroids(Configuration conf, String inputPath, int k, int dataSetSize)
            throws IOException {
        Record[] centroids = new Record[k];
        List<Integer> positions = new ArrayList<>();
        Random rand = new Random(11);

        while (positions.size() < k) {
            int pos = rand.nextInt(dataSetSize);
            if (!positions.contains(pos))
                positions.add(pos);
        }
        Collections.sort(positions);

        Path path = new Path(inputPath);
        FileSystem hdfs = FileSystem.get(conf);

        try (BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(path)))) {
            int row = 0, i = 0;
            String line;
            while ((line = br.readLine()) != null && i < k) {
                if (row == positions.get(i)) {
                    String[] parts = line.split(",");
                    String[] featuresOnly = Arrays.copyOfRange(parts, 0, parts.length - 1);
                    centroids[i++] = new Record(featuresOnly);
                }
                row++;
            }
        }

        return centroids;
    }

    private static Record[] readCentroidsFromHDFS(Configuration conf, int k, String outputPath) throws IOException {
        Record[] centroids = new Record[k];
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] files = hdfs.listStatus(new Path(outputPath));

        for (FileStatus file : files) {
            Path path = file.getPath();
            if (!path.getName().equals("_SUCCESS")) {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(path)))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] keyValue = line.split("\t");
                        int id = Integer.parseInt(keyValue[0]);
                        centroids[id] = new Record(keyValue[1].split(","));
                    }
                }
            }
        }
        hdfs.delete(new Path(outputPath), true);
        return centroids;
    }

    private static void writeFinalCentroids(Configuration conf, Record[] centroids, String output) throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        try (BufferedWriter bw = new BufferedWriter(
                new OutputStreamWriter(hdfs.create(new Path(output + "/centroids.txt"), true)))) {
            for (Record centroid : centroids) {
                bw.write(centroid.toString());
                bw.newLine();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        long startIC, endIC;

        Configuration conf = new Configuration();
        conf.addResource(new Path("config.xml"));
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: <input> <output>");
            System.exit(1);
        }

        final String inputPath = otherArgs[0];
        final String outputPath = otherArgs[1] + "/temp";
        final int datasetSize = conf.getInt("dataset", 150);
        final int k = conf.getInt("k", 4);
        final double threshold = conf.getDouble("threshold", 0.0001);
        final int maxIterations = conf.getInt("max.iteration", 100);

        Record[] oldCentroids = new Record[k];
        Record[] newCentroids = new Record[k];

        // Initialize centroids
        startIC = System.currentTimeMillis();
        newCentroids = initCentroids(conf, inputPath, k, datasetSize);
        endIC = System.currentTimeMillis();
        updateCentroidConfig(conf, newCentroids);

        int iteration = 0;
        boolean converged = false;

        while (!converged && iteration < maxIterations) {
            iteration++;

            Job job = Job.getInstance(conf, "Iteration_" + iteration);
            job.setJarByClass(KmeansDriver.class);
            job.setMapperClass(KmeansMapper.class);
            job.setCombinerClass(KmeansCombiner.class);
            job.setReducerClass(KmeansReducer.class);
            job.setNumReduceTasks(k);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Record.class);

            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            if (!job.waitForCompletion(true)) {
                System.err.println("Iteration " + iteration + " failed.");
                System.exit(1);
            }

            for (int i = 0; i < k; i++) {
                oldCentroids[i] = Record.copy(newCentroids[i]);
            }

            newCentroids = readCentroidsFromHDFS(conf, k, outputPath);
            converged = hasConverged(oldCentroids, newCentroids, threshold);

            if (!converged) {
                updateCentroidConfig(conf, newCentroids);
            }
        }

        writeFinalCentroids(conf, newCentroids, otherArgs[1]);

        long totalTime = System.currentTimeMillis() - start;
        long icTime = endIC - startIC;

        System.out.println("Execution time: " + totalTime + " ms");
        System.out.println("Initial centroid selection time: " + icTime + " ms");
        System.out.println("Total iterations: " + iteration);

        // here I want to read file called centroids.txt from HDFS and construct the
        // clusters
        // read centroids.txt
        // create record for each centroid
        // calculate the clusters based on the nearest distance
        // contruct KmeansWrapper
        // get labels
        // Run Contingency Matrix
        // int[][] matrix = ContingencyMatrix.buildMatrix(KMeansWrapper.getClusters(),
        // labels);
        // ContingencyMatrix.printMatrix(matrix);

        // Read final centroids from centroids.txt
        Path finalCentroidPath = new Path(otherArgs[1] + "/centroids.txt");
        FileSystem hdfs = FileSystem.get(conf);
        List<Cluster> clusters = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(finalCentroidPath)))) {
            String line;
            int clusterIndex = 0;
            while ((line = br.readLine()) != null) {
                String[] features = line.split(",");
                clusters.add(new Cluster(new Record(features), clusterIndex++));
            }
        }

        // Read the original dataset and assign each point to the nearest cluster
        Path dataPath = new Path(inputPath);
        List<String> labels = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(dataPath)))) {
            String line;
            int recordIndex = 0;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                String[] featuresOnly = Arrays.copyOfRange(parts, 0, parts.length - 1);
                Record record = new Record(featuresOnly, recordIndex);
                labels.add(parts[parts.length - 1]); // Collect the actual label
                Cluster nearest = null;
                double minDistance = Double.MAX_VALUE;

                for (Cluster cluster : clusters) {
                    double distance = cluster.getCentroid().distance(record);
                    if (distance < minDistance) {
                        minDistance = distance;
                        nearest = cluster;
                    }
                }

                if (nearest != null) {
                    nearest.addPoint(record);
                }

                recordIndex++;
            }
        }

        // Build and print contingency matrix
        int[][] matrix = ContingencyMatrix.buildMatrix(clusters, labels);

        Path matrixPath = new Path(otherArgs[1] + "/contingency_matrix.txt");
        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(hdfs.create(matrixPath, true)))) {
            for (int i = 0; i < matrix.length; i++) {
                for (int j = 0; j < matrix[i].length; j++) {
                    bw.write(matrix[i][j] + (j == matrix[i].length - 1 ? "" : "\t"));
                }
                bw.newLine();
            }
        }

        ContingencyMatrix.printMatrix(matrix);

        System.exit(0);
    }
}
