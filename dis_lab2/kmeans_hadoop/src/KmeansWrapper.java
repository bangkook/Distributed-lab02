package kmeans_hadoop.src;

import java.util.List;

public class KmeansWrapper {
    private List<Cluster> clusters;

    public KmeansWrapper(List<Cluster> clusters) {
        this.clusters = clusters;
    }

    public List<Cluster> getClusters() {
        return clusters;
    }

    public void setClusters(List<Cluster> clusters) {
        this.clusters = clusters;
    }
}
