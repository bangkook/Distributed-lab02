package unparallelled_kmeans;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import utils.*;
import utils.Record;

public class Main {
    public static void main(String[] args) {
        String filePath = "src/input/iris.data";

        List<Record> records = new ArrayList<>();
        List<String> labels = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            int idx = 0;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(","); // Split by comma
                Double[] featureVector = new Double[values.length - 1]; // Exclude last column for labels

                // Parse features
                for (int i = 0; i < values.length - 1; i++) {
                    featureVector[i] = Double.parseDouble(values[i]);
                }

                records.add(new Record(featureVector, idx++));
                labels.add(values[values.length - 1]); // Last column is the label
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        long startTime = System.nanoTime();
        KMeans kMeans = new KMeans(records, 3, 100);
        kMeans.run();
        long endTime = System.nanoTime();

        // Print final centroids
        System.out.println("Final centroids coordinates: ");
        for (Cluster cluster : kMeans.getClusters()) {
            System.out.println(Arrays.toString(cluster.getCentroid().features()));
        }

        double elapsedTime = (endTime - startTime) / 1e6; // Convert to milliseconds
        System.out.println("K-Means execution time: " + elapsedTime + " ms");

        int[][] matrix = ContingencyMatrix.buildMatrix(kMeans.getClusters(), labels);
        ContingencyMatrix.printMatrix(matrix);
    }
}
