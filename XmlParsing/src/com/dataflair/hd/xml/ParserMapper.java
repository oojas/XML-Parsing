package com.dataflair.hd.xml;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

/*
<configuration>
<property>
<name>fs.default.name</name>
<value>hdfs://localhost:9000</value>
</property>
<property>
<name>hadoop.tmp.dir</name>
<value>/home/hdadmin/hdata/hadoop-${user.name}</value>
<description> 
All the data will be stored at this location. 
</description>
</property>
</configuration>
*/
public class ParserMapper extends Mapper<LongWritable, Text, NullWritable, Text>
{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String xmlString = value.toString();
		SAXBuilder builder = new SAXBuilder();
		Reader in = new StringReader(xmlString);
		try
		{
			Document doc = builder.build(in);
			Element root = doc.getRootElement();					//property
			
			String tag1 = root.getChild("name").getTextTrim() ;
			String tag2 = root.getChild("value").getTextTrim() ;
			context.write(NullWritable.get(), new Text(tag1+ ","+tag2));
//														name, value
		}
		catch (JDOMException ex)
		{
			Logger.getLogger(ParserMapper.class.getName()).log(Level.SEVERE, null, ex);
		}
		catch (IOException ex)
		{
			Logger.getLogger(ParserMapper.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
}