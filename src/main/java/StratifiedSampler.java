import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
    private static final double LONG_LOW = 8.1461259;
    private static final double LONG_HIGH = 11.1993265;
    private static final double LATI_LOW = 56.5824856;
    private static final double LATI_HIGH = 57.750511;

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Data Preprocessing");
        job.setJarByClass(StratifiedSampler.class);

        Configuration fromStrConf = new Configuration(false);
        ChainMapper.addMapper(job, FromString.class, LongWritable.class, Text.class, Text.class, Review.class,
                fromStrConf);

        Configuration samplingConf = new Configuration(false);
        ChainReducer.setReducer(job, StratifiedSampling.class, Text.class, Review.class, Text.class, Review.class,
                samplingConf);

        ChainReducer.addMapper(job, ToString.class, Text.class, Review.class, LongWritable.class, Text.class, null);

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
        public void reduce(Text key, Iterable<Review> reviews, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            double totalRating = 0, totalIncome = 0;
            List<Review> sample = new ArrayList<>();
            for (Review review : reviews) {
                count = count % SAMPLE_RATE;
                if (count == 0) {
                    if (review.longitude >= LONG_LOW && review.longitude <= LONG_HIGH && review.latitude >= LATI_LOW
                            && review.latitude <= LATI_HIGH) {
                        if (review.rating > 0) {
                            review.rating /= 100;
                            totalRating += review.rating;
                        }

                        if (review.user_income > 0) {
                            totalIncome += review.user_income;
                        }
                        try {
                            sample.add((Review) review.clone());
                        } catch (CloneNotSupportedException ignored) {

                        }
                    }
                }
                count++;
            }

            count = sample.size();
            int low = count / 100;
            int high = count - low;
            double avgRating = totalRating / count;
            double avgIncome = totalIncome / count;
            sample.stream().forEach(review -> {
                if (review.rating < 0) {
                    review.rating = avgRating;
                }

                if (review.user_income < 0) {
                    review.user_income = avgIncome;
                }
            });

            sample.sort((r1, r2) -> (int) (r1.rating - r2.rating));
            for (Review review : sample.subList(low, high)) {
                context.write(key, review);
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
