package utils;

import java.util.ArrayList;
import java.util.List;

public class Cluster {
    private Record centroid;
    private final int index;
    private final List<Record> points;

    public Cluster(Record centroid, int index) {
        this.centroid = centroid;
        this.index = index;
        this.points = new ArrayList<>();
    }

    public Record getCentroid() {
        return centroid;
    }

    public int getIndex() {
        return index;
    }

    public List<Record> getPoints() {
        return points;
    }

    public void setCentroid(Record centroid) {
        this.centroid = centroid;
    }

    public void addPoint(Record record) {
        points.add(record);
    }

    public void clearPoints() {
        points.clear();
    }
}
