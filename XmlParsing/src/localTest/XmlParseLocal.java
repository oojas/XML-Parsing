package localTest;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.jdom.input.SAXBuilder;
import org.jdom.xpath.*;

/*
<configuration>
<property>
<name>fs.default.name</name>
<value>hdfs://localhost:9000</value>
</property>
<property>
<name>hadoop.tmp.dir</name>
<value>/home/hdadmin/hdata/hadoop-${user.name}</value>
</property>
</configuration>
*/
public class XmlParseLocal
{
	public static String getMailBody() throws Exception
	{
		String entireFileText = new Scanner(new File("C:\\Users\\santa\\Desktop\\Desk\\santa\\Viznet_Dump_Sample.xml")).useDelimiter("\\A").next();
		return entireFileText;
	}
	public static void main(String[] args) throws IOException, InterruptedException, Exception
	{
		parseXml(getMailBody());
	}
	public static void parseXml(String xmlString) throws IOException, InterruptedException
	{
		SAXBuilder builder = new SAXBuilder();
		Reader in = new StringReader(xmlString);
		try
		{
			Document doc = builder.build(in);
			
			Element root = doc.getRootElement();          

	        Namespace ns = Namespace.getNamespace("ss","urn:schemas-microsoft-com:office:spreadsheet");
	        Namespace ns2 = Namespace.getNamespace("o","urn:schemas-microsoft-com:office:office");
	        Namespace ns3 = Namespace.getNamespace("x","urn:schemas-microsoft-com:office:excel");
	        Namespace ns4 = Namespace.getNamespace("html","http://www.w3.org/TR/REC-html40");
	        Namespace ns5 = Namespace.getNamespace("urn:schemas-microsoft-com:office:spreadsheet");
	        
	        root.addNamespaceDeclaration(ns);
	        root.addNamespaceDeclaration(ns2);
	        root.addNamespaceDeclaration(ns3);
	        root.addNamespaceDeclaration(ns4);
	        root.addNamespaceDeclaration(ns5);
	        
	        List<Element> wsElements = root.getChildren();
	        for (Element wsElement : wsElements)
	        {
	        	System.out.println(wsElement.getName());
	        	if (wsElement.getName().equals("Worksheet"))
	        	{
	        		List<Element> tabElements = wsElement.getChildren();
//	        		System.out.println("c.getChildren().size() " + c.getChildren().size());
	        		for (Element tabElement : tabElements)
	    	        {
//	    	        	System.out.println(tabElement.getName());
	    	        	if (tabElement.getName().equals("Table"))
	    	        	{
	    	        		List<Element> rowElements = tabElement.getChildren();
//	    	        		System.out.println("c.getChildren().size() " + c.getChildren().size());
	    	        		for (Element rowElement : rowElements)
	    	    	        {
//	    	    	        	System.out.println(rowElement.getName());
	    	    	        	if (rowElement.getName().equals("Row"))
	    	    	        	{
	    	    	        		List<Element> cellElements = rowElement.getChildren();
//	    	    	        		System.out.println("c.getChildren().size() " + c.getChildren().size());
	    	    	        		for (Element cellElement : cellElements)
	    	    	    	        {
//	    	    	    	        	System.out.println(cellElement.getName());
	    	    	    	        	if (cellElement.getName().equals("Cell"))
	    	    	    	        	{
	    	    	    	        		List<Element> dataElements = cellElement.getChildren();
//	    	    	    	        		System.out.println("c.getChildren().size() " + c.getChildren().size());
	    	    	    	        		for (Element dataElement : dataElements)
	    	    	    	    	        {
//	    	    	    	    	        	System.out.println(dataElement.getName());
	    	    	    	    	        	if (dataElement.getName().equals("Data"))
	    	    	    	    	        	{
	    	    	    	    	        		System.out.print(dataElement.getText() + "|");
	    	    	    	    	        	}
	    	    	    	    	        }
	    	    	    	        	}
	    	    	    	        }
	    	    	        		System.out.println();
	    	    	        	}
	    	    	        }
	    	        	}
	    	        }
	        	}
	        }

		}
		catch (Exception ex)
		{
			Logger.getLogger(XmlParseLocal.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
}