package utils;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Occurrences implements WritableComparable<Occurrences> {
    private boolean corpus_part;
    private long count;

    public Occurrences(){}

    public Occurrences(boolean corpus, long count){
        this.corpus_part = corpus;
        this.count = count;
    }

    @Override public int compareTo(Occurrences o) {
        return corpusNumericValue() - o.corpusNumericValue();
    }

    @Override public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(corpus_part);
        dataOutput.writeLong(count);
    }

    @Override public void readFields(DataInput dataInput) throws IOException {
        corpus_part = dataInput.readBoolean();
        count = dataInput.readLong();
    }

    public boolean getCorpus_part() {
        return corpus_part;
    }

    public int corpusNumericValue(){
        return corpus_part ? 0 : 1;
    }
    public long getCount() {
        return count;
    }

    public void setCorpus_part(boolean corpus_part) {
        this.corpus_part = corpus_part;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public String toString(){
        return corpus_part + " " + count;
    }

}
