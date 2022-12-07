package utils;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Occurrences implements WritableComparable<Occurrences> {
    private boolean corpus;
    private long count;

    public Occurrences(){}

    public Occurrences(boolean corpus, long count){
        this.corpus = corpus;
        this.count = count;
    }

    @Override public int compareTo(Occurrences o) {
        return corpusNumericValue() - o.corpusNumericValue();
    }

    @Override public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(corpus);
        dataOutput.writeLong(count);
    }

    @Override public void readFields(DataInput dataInput) throws IOException {
        corpus = dataInput.readBoolean();
        count = dataInput.readLong();
    }

    public boolean isCorpus() {
        return corpus;
    }

    public int corpusNumericValue(){
        return corpus ? 0 : 1;
    }
    public long getCount() {
        return count;
    }

    public void setCorpus(boolean corpus) {
        this.corpus = corpus;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public String toString(){
        return corpus + " " + count;
    }

}
