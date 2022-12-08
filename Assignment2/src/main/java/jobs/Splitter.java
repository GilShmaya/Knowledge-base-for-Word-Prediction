package jobs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import utils.Occurrences;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
    The Splitter job is responsible for the following:
    1. Divide the corpus.
    2. Count the occurrences and calculating N (N is number of word sequences of size 3 in the corpus).
    3. Calculate R of the two parts of the corpus for each trigram.
*/
public class Splitter {


    /*
       Map every line into <Trigram, Occurrences>, the Occurrences include a division of the corpus to two parts and
       the occurrences of every trigram.
     */
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Occurrences> {
        private static final Pattern ENGLISH = Pattern.compile("(?<trigram>[A-Z]+ [A-Z]+ [A-Z]+)\\t\\d{4}\\t" +
                "(?<occurrences>\\d+).*");

        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            Matcher matcher = ENGLISH.matcher(line.toString());
            if (matcher.matches()) {
                context.write(new Text(matcher.group("trigram")), new Occurrences((lineId.get() % 2 == 0),
                        Long.parseLong(matcher.group("occurrences"))));
            }
        }
    }


    /*
        Combine the Occurrences the mapper created.
     */
    public static class CombinerClass extends Reducer<Text, Occurrences, Text, Occurrences> {
        private long r1;
        private long r2;
        private String text;

        public void setup(Context context) {
            r1 = 0;
            r2 = 0;
            text = "";
        }

        public void reduce(Text key, Iterable<Occurrences> values, Context context) throws IOException, InterruptedException {
            for (Occurrences value : values) {  // init
                if (!key.toString().equals(text)) {
                    r1 = 0;
                    r2 = 0;
                    text = key.toString();
                }
                if (value.getCorpus_part()) {
                    r1 = r1 + value.getCount();
                } else {
                    r2 = r2 + value.getCount();
                }
            }
            context.write(new Text(text), new Occurrences(true, r1));
            context.write(new Text(text), new Occurrences(false, r2));
        }
    }

    public static class ReducerClass extends Reducer<Text, Occurrences, Text, Text> {
        private long r1;
        private long r2;
        private String text;

        enum Counter {
            N
        }

        public void setup(Context context) {
            r1 = 0;
            r2 = 0;
            text = "";
        }

        public void reduce(Text key, Iterable<Occurrences> values, Context context) throws IOException, InterruptedException {
            for (Occurrences value : values) {
                context.getCounter(Counter.N).increment(value.getCount());
                if (!key.toString().equals(text)) { // init
                    r1 = 0;
                    r2 = 0;
                    text = key.toString();
                }
                if (value.getCorpus_part()) {
                    r1 = r1 + value.getCount();
                } else {
                    r2 = r2 + value.getCount();
                }
            }
            context.write(new Text(text), new Text(r1 + " " + r2));
        }
    }


    public static class PartitionerClass extends Partitioner<Text, Occurrences> {
        public int getPartition(Text key, Occurrences value, int partitionsNumber) {
            return key.hashCode() % partitionsNumber;
        }
    }

}
