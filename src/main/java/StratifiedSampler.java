import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StratifiedSampler {
    private static final int SAMPLE_RATE = 100;

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Data Preprocessing");
        job.setJarByClass(StratifiedSampler.class);

        Configuration fromStrConf  = new Configuration(false);
        ChainMapper.addMapper(job, FromString.class, LongWritable.class, Text.class, 
                Text.class, Review.class, fromStrConf);

        Configuration samplingConf = new Configuration(false);
        ChainReducer.setReducer(job, StratifiedSampling.class, Text.class, Review.class, 
                Text.class, Review.class, samplingConf);

        ChainReducer.addMapper(job, ToString.class, Text.class, Review.class, 
                LongWritable.class, Text.class, null);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class FromString extends Mapper<LongWritable, Text, Text, Review> {
        Review review = new Review();
        private Text USER_CAREER = new Text();

        @Override
        public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
            review.fromString(text.toString());
            USER_CAREER.set(review.user_career);
            context.write(USER_CAREER, review);
        }
    }

    public static class StratifiedSampling extends Reducer<Text, Review, Text, Review> {
        @Override
        public void reduce(Text key, Iterable<Review> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (Review review : values) {
                count = count % SAMPLE_RATE;
                if (count == 0) {
                    context.write(key, review);
                }
                count++;
            }
        }
    }

    public static class ToString extends Mapper<Text, Review, LongWritable, Text> {
        private Text text = new Text();

        @Override
        public void map(Text key, Review review, Context context) throws IOException, InterruptedException {
            text.set(review.toString());
            context.write(null, text);
        }
    }
}
