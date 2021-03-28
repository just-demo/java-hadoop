package demo;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.nio.file.Paths;

public class WordCount extends Configured implements Tool{

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new WordCount(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        String inFile = Paths.get("data").resolve("in").resolve("in.txt").toAbsolutePath().toString();
        String outDir = Paths.get("data").resolve("out").toAbsolutePath().toString();
        FileUtils.deleteDirectory(new File(outDir));

        Job job = new Job();
        job.setJarByClass(WordCount.class);
        job.setJobName("WordCounter");

        FileInputFormat.addInputPath(job, new Path(inFile));
        FileOutputFormat.setOutputPath(job, new Path(outDir));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);

        int result = job.waitForCompletion(true) ? 0 : 1;
        System.out.println("Job result: " + result + " / " + job.isSuccessful());
        return result;
    }
}