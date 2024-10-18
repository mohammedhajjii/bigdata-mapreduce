package md.hajji.sales;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * type parameters:
 * LongWritable: is the type of the Key of input, which will be the Index of first char in line.
 * Text: is the type of the value, in this case we get a line
 * Text: The type of the output key, which will be city name.
 * DoubleWritable: the type of the output value, which will be the price of soling product
 */

public class SaleByCityMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        // split line into tokens:
        String[] tokens = value.toString().split(" ");
        // getting the city and product price value
        String city = tokens[1];
        double price = Double.parseDouble(tokens[3]);
        // pass the resolved details to the next Reducer
        context.write(new Text(city), new DoubleWritable(price));

    }
}
