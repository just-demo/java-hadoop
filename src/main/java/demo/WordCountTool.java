package demo;

import org.apache.commons.io.FileUtils;
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

import java.io.File;
import java.nio.file.Paths;

public class WordCountTool extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WordCountTool(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        String inFile = Paths.get("data").resolve("in").resolve("in.txt").toAbsolutePath().toString();
        String outDir = Paths.get("data").resolve("out").toAbsolutePath().toString();
        FileUtils.deleteDirectory(new File(outDir));

        Job job = Job.getInstance();
        job.setJarByClass(WordCountTool.class);
        job.setJobName("WordCounter");

        FileInputFormat.addInputPath(job, new Path(inFile));
        FileOutputFormat.setOutputPath(job, new Path(outDir));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}