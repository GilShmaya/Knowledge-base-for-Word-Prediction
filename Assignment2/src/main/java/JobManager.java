import jobs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;
import utils.Aggregator;
import utils.NewProbability;
import utils.Occurrences;

import java.io.IOException;

public class JobManager {
    public static void main(String[] args) {

        try {
            BasicConfigurator.configure();

            // Split the Google Books Ngrams into 2 corpus
            Configuration confSplitter = new Configuration();
            Job splitterJob = Job.getInstance(confSplitter, "Splitter");
            splitterJob.setJarByClass(Splitter.class);

            splitterJob.setMapperClass(Splitter.MapperClass.class);
            splitterJob.setCombinerClass(Splitter.CombinerClass.class);
            splitterJob.setPartitionerClass(Splitter.PartitionerClass.class);
            splitterJob.setReducerClass(Splitter.ReducerClass.class);

            splitterJob.setMapOutputKeyClass(Text.class);
            splitterJob.setMapOutputValueClass(Occurrences.class);

            splitterJob.setOutputKeyClass(Text.class);
            splitterJob.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(splitterJob, new Path(args[1])); // Google Book ngram file
            splitterJob.setInputFormatClass(SequenceFileInputFormat.class);

            FileOutputFormat.setOutputPath(splitterJob,new Path("s3n://assignment2gy/Step1"));
            splitterJob.setOutputFormatClass(TextOutputFormat.class);

            if (splitterJob.waitForCompletion(true)){
                System.out.println("Corpus splitter succeed!");
            }
            Counters counters = splitterJob.getCounters();
            Counter counter = counters.findCounter(Splitter.ReducerClass.Counter.N);
            long N = counter.getValue();


            // Calculate Nr & Tr
            Configuration confNrTr = new Configuration();
            Job NrTrCalculatorJob = Job.getInstance(confNrTr, "NrTrCalculator");
            NrTrCalculatorJob.setJarByClass(NrTrCalculator.class);

            NrTrCalculatorJob.setMapperClass(NrTrCalculator.MapperClass.class);
            NrTrCalculatorJob.setPartitionerClass(NrTrCalculator.PartitionerClass.class);
            NrTrCalculatorJob.setReducerClass(NrTrCalculator.ReducerClass.class);

            NrTrCalculatorJob.setMapOutputKeyClass(LongWritable.class);
            NrTrCalculatorJob.setMapOutputValueClass(Aggregator.class);

            NrTrCalculatorJob.setOutputKeyClass(LongWritable.class);
            NrTrCalculatorJob.setOutputValueClass(Aggregator.class);

            FileInputFormat.addInputPath(NrTrCalculatorJob, new Path("s3n://assignment2gy/Step1"));
            NrTrCalculatorJob.setInputFormatClass(TextInputFormat.class);

            FileOutputFormat.setOutputPath(NrTrCalculatorJob,new Path("s3n://assignment2gy/Step2"));
            NrTrCalculatorJob.setOutputFormatClass(TextOutputFormat.class);

            if (NrTrCalculatorJob.waitForCompletion(true)){
                System.out.println("Nr & Tr calculation complete!");
            }


            // Join Trigram Nr & Tr
            Configuration confJoiner = new Configuration();
            Job joinerJob = Job.getInstance(confJoiner, "Joiner");
            joinerJob.setJarByClass(Joiner.class);

            joinerJob.setMapperClass(Joiner.MapperClass.class);
            joinerJob.setPartitionerClass(Joiner.PartitionerClass.class);
            joinerJob.setReducerClass(Joiner.ReducerClass.class);

            joinerJob.setMapOutputKeyClass(Text.class);
            joinerJob.setMapOutputValueClass(Text.class);

            joinerJob.setOutputKeyClass(Text.class);
            joinerJob.setOutputValueClass(Text.class);

            MultipleInputs.addInputPath(joinerJob, new Path("s3n://assignment2gy/Step1"),TextInputFormat.class,
                    Joiner.MapperClass.class);
            MultipleInputs.addInputPath(joinerJob, new Path("s3n://assignment2gy/Step2"),TextInputFormat.class,
                    Joiner.MapperClass.class);
            FileOutputFormat.setOutputPath(joinerJob,new Path("s3n://assignment2gy/Step3"));
            joinerJob.setOutputFormatClass(TextOutputFormat.class);

            if (joinerJob.waitForCompletion(true)){
                System.out.println("Joining Tr Nr job completed!");
            }


            // Calculating probability
            Configuration confNewProbability = new Configuration();
            confNewProbability.setLong("N",N);
            Job deProbabilityJob = Job.getInstance(confNewProbability, "DEprobability");
            deProbabilityJob.setJarByClass(DEprobability.class);

            deProbabilityJob.setMapperClass(DEprobability.MapperClass.class);
            deProbabilityJob.setPartitionerClass(DEprobability.PartitionerClass.class);
            deProbabilityJob.setReducerClass(DEprobability.ReducerClass.class);

            deProbabilityJob.setMapOutputKeyClass(Text.class);
            deProbabilityJob.setMapOutputValueClass(Text.class);
            deProbabilityJob.setOutputKeyClass(Text.class);
            deProbabilityJob.setOutputValueClass(DoubleWritable.class);

            MultipleInputs.addInputPath(deProbabilityJob, new Path("s3n://assignment2gy/Step3"),TextInputFormat.class
                    ,DEprobability.MapperClass.class);
            MultipleOutputs.addNamedOutput(deProbabilityJob,"probs",TextOutputFormat.class,Text.class,DoubleWritable.class);
            FileOutputFormat.setOutputPath(deProbabilityJob,new Path("s3n://assignment2gy/Step4"));
            deProbabilityJob.setOutputFormatClass(TextOutputFormat.class);

            if (deProbabilityJob.waitForCompletion(true)){
                System.out.println("Calculate the Deleted Estimation Probability done!");
            }


            // Sort the result
            Configuration confSortOutput = new Configuration();
            Job sortOutputJob = Job.getInstance(confSortOutput, "SortOutput");
            sortOutputJob.setJarByClass(SortOutput.class);

            sortOutputJob.setMapperClass(SortOutput.MapperClass.class);
            sortOutputJob.setPartitionerClass(SortOutput.PartitionerClass.class);
            sortOutputJob.setReducerClass(SortOutput.ReducerClass.class);

            sortOutputJob.setMapOutputKeyClass(NewProbability.class);
            sortOutputJob.setMapOutputValueClass(Text.class);

            sortOutputJob.setOutputKeyClass(Text.class);
            sortOutputJob.setOutputValueClass(Text.class);

            MultipleInputs.addInputPath(sortOutputJob, new Path("s3n://assignment2gy/Step4"),TextInputFormat.class,
                    SortOutput.MapperClass.class);
            MultipleOutputs.addNamedOutput(sortOutputJob,"Result",TextOutputFormat.class,Text.class,Text.class);
            FileOutputFormat.setOutputPath(sortOutputJob,new Path(args[2]));
            sortOutputJob.setOutputFormatClass(TextOutputFormat.class);

            if (sortOutputJob.waitForCompletion(true)){
                System.out.println("Done sorting output!!");
            }

        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}