package md.hajji.logs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class LogsMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private String ipAddress;
    private final Logger logger = Logger.getLogger(LogsMapper.class);
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        // get the IP address from the context:
        ipAddress = context.getConfiguration().get("ipAddress");
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {

        // compute the request in order to add it to total request number:
        context.write(new Text("total-request"), new LongWritable(1));

        // split request line to tokens and parse the IP address:
        String[] tokens = value.toString().split(" -- ");
        String ipAddress = tokens[0];

        // ignore if no match between the request IP address and current
        // IP address:
        if (!ipAddress.equals(this.ipAddress))
            return;

        // parse the status code:
        int statusCode = Integer.parseInt(tokens[1].split(" ")[5]);

        // if the request was failed:
        if (statusCode != 200)
            return;

        // if not, add the request to IP address list:
        context.write(new Text(ipAddress), new LongWritable(1));
    }
}
