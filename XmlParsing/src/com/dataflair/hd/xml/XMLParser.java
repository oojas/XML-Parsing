package com.dataflair.hd.xml;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class XMLParser
{
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();

		conf.set("xmlinput.start", "<property>");
		conf.set("xmlinput.end", "</property>");
		conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");

		Job job = new Job(conf, "XML Parsing");

		FileInputFormat.setInputPaths(job, args[0]);
		job.setJarByClass(XMLParser.class);
		job.setMapperClass(ParserMapper.class);
		job.setNumReduceTasks(0);										//no of reducers = 0
		job.setInputFormatClass(XmlInputFormat.class);					//set Input Format
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		org.apache.hadoop.filecache.DistributedCache.addFileToClassPath(new Path("/jars/jdom.jar"), job.getConfiguration(), FileSystem.get(conf));

		Path outPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPath);
		FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
		if (dfs.exists(outPath))
		{
			dfs.delete(outPath, true);
		}

		job.waitForCompletion(true);

	}
}






/*

		try
		{
			runJob(args[0], args[1]);
		}
		catch (IOException ex)
		{
			Logger.getLogger(ParserDriver.class.getName()).log(Level.SEVERE, null, ex);
		}

	}

	public static void runJob(String input, String output) throws IOException {

		Configuration conf = new Configuration();

		conf.set("xmlinput.start", "<property>");
		conf.set("xmlinput.end", "</property>");
		conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");

		Job job = new Job(conf, "XML Parsing");

		FileInputFormat.setInputPaths(job, input);
		job.setJarByClass(ParserDriver.class);
		job.setMapperClass(MyParserMapper.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		org.apache.hadoop.filecache.DistributedCache.addFileToClassPath(new Path("/jars/jdom.jar"), job.getConfiguration(), FileSystem.get(conf));

		Path outPath = new Path(output);
		FileOutputFormat.setOutputPath(job, outPath);
		FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
		if (dfs.exists(outPath)) {
			dfs.delete(outPath, true);
		}

		try {

			job.waitForCompletion(true);

		} catch (InterruptedException ex) {
			Logger.getLogger(ParserDriver.class.getName()).log(Level.SEVERE,
					null, ex);
		} catch (ClassNotFoundException ex) {
			Logger.getLogger(ParserDriver.class.getName()).log(Level.SEVERE,
					null, ex);
		}

	}
*/
