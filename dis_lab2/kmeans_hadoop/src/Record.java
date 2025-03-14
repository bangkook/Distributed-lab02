package kmeans_hadoop.src;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Record implements Writable {

    private Double[] features = null;
    private int dim;
    private int numRecords;
    private int index;

    public Record() {
        this.dim = 0;
    }

    public Record(final Double[] c) {
        this.set(c);
    }

    public Record(final String[] s) {
        this.set(s);
    }

    public Record(final String[] s, int index) {
        this.set(s, index);
    }

    public static Record copy(final Record record) {
        Record ret = new Record(record.features);
        ret.numRecords = record.numRecords;
        return ret;
    }

    public void set(final Double[] c) {
        this.features = c;
        this.dim = c.length;
        this.numRecords = 1;
    }

    public void set(final String[] s) {
        this.features = new Double[s.length];
        this.dim = s.length;
        this.numRecords = 1;
        for (int i = 0; i < s.length; i++) {
            this.features[i] = Double.parseDouble(s[i]);
        }
    }

    public void set(final String[] s, int index) {
        this.features = new Double[s.length];
        this.dim = s.length;
        this.numRecords = 1;
        this.index = index;
        for (int i = 0; i < s.length; i++) {
            this.features[i] = Double.parseDouble(s[i]);
        }
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        this.dim = in.readInt();
        this.numRecords = in.readInt();
        this.features = new Double[this.dim];

        for (int i = 0; i < this.dim; i++) {
            this.features[i] = in.readDouble();
        }
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeInt(this.dim);
        out.writeInt(this.numRecords);

        for (int i = 0; i < this.dim; i++) {
            out.writeDouble(this.features[i]);
        }
    }

    @Override
    public String toString() {
        StringBuilder Record = new StringBuilder();
        for (int i = 0; i < this.dim; i++) {
            Record.append(Double.toString(this.features[i]));
            if (i != dim - 1) {
                Record.append(",");
            }
        }
        return Record.toString();
    }

    // to sum 2 records
    public void sum(Record record) {
        for (int i = 0; i < this.dim; i++) {
            this.features[i] += record.features[i];
        }
        this.numRecords += record.numRecords;
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

    public int index() {
        return this.index;
    }
}