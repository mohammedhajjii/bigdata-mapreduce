package md.hajji.sales;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class SaleByCityOfYearMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private int year;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        // get year value from configuration context:
        year = context.getConfiguration().getInt("year", 2022);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        // split line into tokens:
        String[] tokens = value.toString().split(" ");
        //parse the soling date:
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");
        LocalDate date = LocalDate.parse(tokens[0], formatter);

        if (date.getYear() != year)
            return;

        // parse price:
        double price = Double.parseDouble(tokens[3]);
        // forward city and price to the reducer:
        context.write(new Text(tokens[1]), new DoubleWritable(price));
    }
}
