package kmeans_hadoop.src;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Record implements Writable {

    private Double[] features = null;
    private int[] indexes = null;
    private int dim;
    private int numRecords;

    public Record() {
        this.dim = 0;
    }

    public Record(final Double[] c, final int[] index) {
        this.set(c, index);
    }

    public Record(final String[] s, final int[] index) {
        this.set(s, index);
    }

    public static Record copy(final Record record) {
        Record ret = new Record(record.features, record.indexes);
        ret.numRecords = record.numRecords;
        return ret;
    }

    public void set(final Double[] c, final int[] index) {
        this.features = c;
        this.dim = c.length;
        this.indexes = index;
        this.numRecords = 1;
    }

    public void set(final String[] s, final int[] index) {
        this.features = new Double[s.length];
        this.dim = s.length;
        this.numRecords = 1;
        this.indexes = index;
        for (int i = 0; i < s.length; i++) {
            this.features[i] = Double.parseDouble(s[i]);
        }
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        this.dim = in.readInt();
        this.numRecords = in.readInt();
        this.features = new Double[this.dim];
        this.indexes = new int[this.numRecords];

        for (int i = 0; i < this.dim; i++) {
            this.features[i] = in.readDouble();
        }

        for (int i = 0; i < this.numRecords; i++) {
            this.indexes[i] = in.readInt();
        }
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeInt(this.dim);
        out.writeInt(this.numRecords);

        for (int i = 0; i < this.dim; i++) {
            out.writeDouble(this.features[i]);
        }

        for (int i = 0; i < this.numRecords; i++) {
            out.writeInt(this.indexes[i]);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        // Append features
        sb.append("Features: [");
        for (int i = 0; i < this.dim; i++) {
            sb.append(this.features[i]);
            if (i != this.dim - 1) {
                sb.append(", ");
            }
        }
        sb.append("]");

        // Append indexes
        sb.append(" | Indexes: [");
        if (this.indexes != null) {
            for (int i = 0; i < this.indexes.length; i++) {
                sb.append(this.indexes[i]);
                if (i != this.indexes.length - 1) {
                    sb.append(", ");
                }
            }
        }
        sb.append("]");

        return sb.toString();
    }

    // to sum 2 records
    public void sum(Record record) {
        for (int i = 0; i < this.dim; i++) {
            this.features[i] += record.features[i];
        }
        this.numRecords += record.numRecords;

        // Merge indexes
        int[] mergedIndexes = new int[this.indexes.length + record.indexes.length];
        System.arraycopy(this.indexes, 0, mergedIndexes, 0, this.indexes.length);
        System.arraycopy(record.indexes, 0, mergedIndexes, this.indexes.length, record.indexes.length);
        this.indexes = mergedIndexes;
    }

    public Double distance(Record record) {
        Double dist = 0.0;
        for (int i = 0; i < this.dim; i++) {
            dist += Math.pow(this.features[i] - record.features[i], 2);
        }
        return Math.sqrt(dist);
    }

    public void average() {
        for (int i = 0; i < this.dim; i++) {
            this.features[i] = this.features[i] / this.numRecords;
        }
        this.numRecords = 1;
    }

    public int[] getIndexes() {
        return this.indexes;
    }

    public String getFeaturesString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < features.length; i++) {
            sb.append(features[i]);
            if (i < features.length - 1) sb.append(",");
        }
        return sb.toString();
    }
    
    public String getIndexesString() {
        if (indexes == null || indexes.length == 0) return "";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < indexes.length; i++) {
            sb.append(indexes[i]);
            if (i < indexes.length - 1) sb.append(",");
        }
        return sb.toString();
    }
    
}