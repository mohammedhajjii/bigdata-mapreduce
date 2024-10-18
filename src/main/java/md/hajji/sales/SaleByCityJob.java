package md.hajji.sales;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

public class SaleByCityJob {

    private static final Logger logger = Logger.getLogger(SaleByCityJob.class);

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        logger.info("Starting SaleByCityJob");
        logger.info("input file: " + args[0]);

        // create MapReduce Job and setting name and jar class:
        Job saleByCityJob = Job.getInstance(new Configuration(), "SaleByCityJob");
        saleByCityJob.setJarByClass(SaleByCityJob.class);

        // set the input type which will be a text file:
        saleByCityJob.setInputFormatClass(TextInputFormat.class);

        // setting the appropriate mapper and reducer:
        saleByCityJob.setMapperClass(SaleByCityMapper.class);
        saleByCityJob.setReducerClass(SaleByCityReducer.class);

        // describe the output key and value types for mapping stage:
        saleByCityJob.setMapOutputKeyClass(Text.class);
        saleByCityJob.setMapOutputValueClass(DoubleWritable.class);

        // describe the output key and value types for reduce stage:
        saleByCityJob.setOutputKeyClass(Text.class);
        saleByCityJob.setOutputValueClass(DoubleWritable.class);

        // setting the input and output files:
        FileInputFormat.addInputPath(saleByCityJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(saleByCityJob, new Path(args[1]));

        // wait job to finish:
        saleByCityJob.waitForCompletion(false);
        logger.info("SaleByCityJob completed");
        logger.info("Output file: " + args[1]);
    }
}
