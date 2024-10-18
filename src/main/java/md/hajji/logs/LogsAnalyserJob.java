package md.hajji.logs;

import md.hajji.sales.SaleByCityJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

public class LogsAnalyserJob {

    private static final Logger logger = Logger.getLogger(SaleByCityJob.class);

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        logger.info("Starting LogsAnalyserJob");
        logger.info("input file: " + args[0]);
        logger.info("searching logs for IP address: " + args[2]);

        // put the IP address argument in configuration, so
        // the mapper can access to it:
        Configuration configuration = new Configuration();
        configuration.set("ipAddress", args[2]);

        // create MapReduce Job and setting name and jar class:
        Job logsJob = Job.getInstance(configuration, "Logs analyser Job");
        logsJob.setJarByClass(LogsAnalyserJob.class);

        // set the input type which will be a text file:
        logsJob.setInputFormatClass(TextInputFormat.class);

        // setting the appropriate mapper and reducer:
        logsJob.setMapperClass(LogsMapper.class);
        logsJob.setReducerClass(LogsReducer.class);


        // describe the output key and value types for mapping stage:
        logsJob.setMapOutputKeyClass(Text.class);
        logsJob.setMapOutputValueClass(LongWritable.class);

        // describe the output key and value types for reduce stage:
        logsJob.setOutputKeyClass(Text.class);
        logsJob.setOutputValueClass(LongWritable.class);

        // setting the input and output files:
        FileInputFormat.addInputPath(logsJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(logsJob, new Path(args[1]));

        // wait job to finish:
        logsJob.waitForCompletion(false);

        logger.info("LogsAnalyserJob completed");
        logger.info("Output file: " + args[1]);
    }
}
