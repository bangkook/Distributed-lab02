package unparallelled_kmeans;

import java.util.*;
import utils.*;
import utils.Record;

public class KMeans {
    private static final Random random = new Random(11);

    private final int k;
    private final List<Record> records;
    private final int maxIterations;
    private List<Cluster> clusters;

    public KMeans(List<Record> records, int k, int maxIterations) {
        this.records = records;
        this.k = k;
        this.maxIterations = maxIterations;
        this.clusters = new ArrayList<>();
    }

    private void initCentroids() {
        Set<Integer> chosenIndexes = new HashSet<>();
        while (chosenIndexes.size() < k) {
            int idx = random.nextInt(records.size());
            if (chosenIndexes.add(idx)) { // Ensure unique centroids
                clusters.add(new Cluster(records.get(idx), clusters.size()));
            }
        }
    }

    private void assignClusters() {
        // Clear previous cluster points
        for (Cluster cluster : clusters) {
            cluster.clearPoints();
        }

        // Assign each record to the nearest cluster
        for (Record record : records) {
            Cluster nearest = nearestCluster(record);
            nearest.addPoint(record);
        }
    }

    private Cluster nearestCluster(Record record) {
        double minDist = Double.MAX_VALUE;
        Cluster nearest = null;

        for (Cluster cluster : clusters) {
            double dist = calculateDistance(record.features(), cluster.getCentroid().features());
            if (dist < minDist) {
                minDist = dist;
                nearest = cluster;
            }
        }
        return nearest;
    }

    private double calculateDistance(Double[] f1, Double[] f2) {
        double dist = 0;
        for (int i = 0; i < f1.length; i++) {
            dist += Math.pow(f1[i] - f2[i], 2);
        }
        return Math.sqrt(dist);
    }

    private List<Record> getNewCentroids() {
        List<Record> newCentroids = new ArrayList<>();
        for (Cluster cluster : clusters) {
            if (cluster.getPoints().isEmpty()) {
                // If cluster is empty, reassign a random point to avoid disappearing clusters
                newCentroids.add(records.get(random.nextInt(records.size())));
            } else {
                newCentroids.add(getCentroid(cluster.getPoints()));
            }
        }
        return newCentroids;
    }

    private Record getCentroid(List<Record> records) {
        Double[] sum = new Double[records.get(0).features().length];
        Arrays.fill(sum, 0.0);

        for (Record record : records) {
            for (int i = 0; i < sum.length; i++) {
                sum[i] += record.features()[i];
            }
        }

        for (int i = 0; i < sum.length; i++) {
            sum[i] /= records.size();
        }
        return new Record(sum, 0);
    }

    public void run() {
        initCentroids();
        for (int i = 0; i < maxIterations; i++) {
            assignClusters();

            List<Record> newCentroids = getNewCentroids();
            boolean converged = true;

            for (int j = 0; j < k; j++) {
                if (!Arrays.equals(newCentroids.get(j).features(), clusters.get(j).getCentroid().features())) {
                    clusters.get(j).setCentroid(newCentroids.get(j));
                    converged = false;
                }
            }

            if (converged) {
                System.out.println("Converged after " + (i + 1) + " iteration(s).");
                break;
            }
        }
    }

    public List<Cluster> getClusters() {
        return clusters;
    }
}
