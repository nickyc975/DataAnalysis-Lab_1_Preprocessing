import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StratifiedSampler {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Data Preprocessing");
        job.setJarByClass(StratifiedSampler.class);
        job.setMapperClass(StratifiedSamplingMapper.class);
        job.setReducerClass(StratifiedSamplingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Review.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class StratifiedSamplingMapper extends Mapper<Object, Text, Text, Review> {
        Review review = new Review();
        private Text USER_CAREER = new Text();

        @Override
        public void map(Object key, Text text, Context context) throws IOException, InterruptedException {
            review.readFields(text.toString());
            USER_CAREER.set(review.user_career);
            context.write(USER_CAREER, review);
        }
    }

    public static class StratifiedSamplingReducer extends Reducer<Text, Review, Text, Review> {
        @Override
        public void reduce(Text key, Iterable<Review> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (Review review : values) {
                count = count % 100;
                if (count == 0) {
                    context.write(null, review);
                }
                count++;
            }
        }
    }
}
