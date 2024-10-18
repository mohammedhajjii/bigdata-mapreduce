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

public class SaleByCityAndYearJob {

    private static final Logger logger = Logger.getLogger(SaleByCityJob.class);


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        logger.info("Starting SaleByCityAndYearJob");
        logger.info("input file: " + args[0]);
        logger.info("searching sales for year: " + args[2]);

        Configuration configuration = new Configuration();

        // put the year argument in configuration, so
        // the mapper can access to year argument:
        configuration.set("year", args[2]);

        // create MapReduce Job and setting name and jar class:
        Job saleByCityAndYearJob = Job.getInstance(configuration, "SaleByCityAndYearJob");
        saleByCityAndYearJob.setJarByClass(SaleByCityAndYearJob.class);

        // set the input type which will be a text file:
        saleByCityAndYearJob.setInputFormatClass(TextInputFormat.class);

        // setting the appropriate mapper and reducer:
        saleByCityAndYearJob.setMapperClass(SaleByCityOfYearMapper.class);
        saleByCityAndYearJob.setReducerClass(SaleByCityReducer.class);

        // describe the output key and value types for mapping stage:
        saleByCityAndYearJob.setMapOutputKeyClass(Text.class);
        saleByCityAndYearJob.setMapOutputValueClass(DoubleWritable.class);

        // describe the output key and value types for reduce stage:
        saleByCityAndYearJob.setOutputKeyClass(Text.class);
        saleByCityAndYearJob.setOutputValueClass(DoubleWritable.class);

        // setting the input and output files:
        FileInputFormat.addInputPath(saleByCityAndYearJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(saleByCityAndYearJob, new Path(args[1]));

        // wait job to finish:
        saleByCityAndYearJob.waitForCompletion(false);
        logger.info("SaleByCityAndYearJob completed");
        logger.info("Output file: " + args[1]);
    }
}
