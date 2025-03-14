package kmeans_hadoop.src;

import java.util.*;

public class ContingencyMatrix {
    public static int[][] buildMatrix(List<Cluster> clusters, List<String> trueLabels) {
        // Step 1: Create a mapping of true labels to indices
        Map<String, Integer> labelMap = new HashMap<>();
        int labelIndex = 0;
        for (String label : trueLabels) {
            if (!labelMap.containsKey(label)) {
                labelMap.put(label, labelIndex++);
            }
        }
        int numClusters = clusters.size();
        int numClasses = labelMap.size();

        // Step 2: Initialize contingency matrix
        int[][] matrix = new int[numClusters][numClasses];

        // Step 3: Fill the matrix using correct record indices
        for (Cluster cluster : clusters) {
            int clusterIdx = cluster.getIndex();
            for (Record record : cluster.getPoints()) {
                int recordIndex = record.index(); // Get index from record
                String trueLabel = trueLabels.get(recordIndex); // Get correct label
                int classIdx = labelMap.get(trueLabel);
                matrix[clusterIdx][classIdx]++;
            }
        }

        return matrix;
    }

    public static void printMatrix(int[][] matrix) {
        System.out.println("Contingency Matrix:");
        for (int[] row : matrix) {
            System.out.println(Arrays.toString(row));
        }
    }
}