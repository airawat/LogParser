package logparser;

import java.util.Properties;
import java.util.Collections;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.hadoop.GlobHfs;
import cascading.tap.MultiSourceTap;
import cascading.tap.hadoop.Hfs;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import cascading.operation.aggregator.Count;
import cascading.operation.expression.ExpressionFunction;

public class LogParser {
	
	//TODO:
	//The year is missing in the data as it is part of directory name
	//a) Figure out how to capture this like we do in Java MR using the fileSplit
	//b) Add the year as a field

	
	public static void main(String[] args) {
	
	    // {{
	    // INSTANTIATE/INITIALIZE
		
	    Properties properties = new Properties();
	    AppProps.setApplicationJarClass( properties, LogParser.class );
	    
	    HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);
	    
	   // Arguments
	    String inputPath = args[ 0 ];
	    String outputPath = args[ 1 ];
	    String errorPath = args[ 2 ];
	    String reportPath = args[ 3 ];
	    
	    // Scheme for sinks
	    TextLine sinkTextLineScheme = new TextLine();
	    
	    // Define what the input file looks like, "offset" is bytes from beginning
	    TextLine sourceTextLineScheme = new TextLine( new Fields( "offset", "line" ) );
	    
	    // The inputPath is a file glob, and a so, GlobHfs is used below
	    GlobHfs sourceFilesGlob = new GlobHfs( sourceTextLineScheme, inputPath );
	    
	    // Create SOURCE tap to read a resource from the HDFS glob
	    Tap sourceSyslogTap = new MultiSourceTap(sourceFilesGlob);
	    
	    // Create a SINK tap to write parsed logs to HDFS
	    sinkTextLineScheme.setNumSinkParts(2);
	    Tap sinkParsedLogTap = new Hfs( sinkTextLineScheme, outputPath, SinkMode.REPLACE);
	    
	    // Create a SINK tap to write reports to HDFS
	    sinkTextLineScheme.setNumSinkParts(1);
	    Tap sinkReportTap = new Hfs(sinkTextLineScheme, reportPath, SinkMode.REPLACE );
	
	    // Create a TRAP tap to write records that failed parsing
	    sinkTextLineScheme.setNumSinkParts(1);
	    Tap sinkTrapTap = new Hfs( sinkTextLineScheme, errorPath , SinkMode.REPLACE );
	    // }}

	    // {{
	    // EXTRACT/PARSE
	    // Declare the field names we will parse out of the log file
	    Fields sysLogFields = new Fields( "month", "day", "time", "node", "process", "message" );

	    // Define the regex pattern
	    String sysLogRegex = "(\\w+)\\s+(\\d+)\\s+(\\d+:\\d+:\\d+)\\s+(\\w+\\W*\\w*)\\s+(.*?\\:)\\s+(.*$)";
	
	    // Declare the groups from the above regex we want to keep. Each regex group will be given
	    // a field name from 'sysLogFields', above, respectively
	    int[] keepParsedGroups = {1, 2, 3, 4, 5, 6};

	    // Create the parser
	    RegexParser parser = new RegexParser( sysLogFields, sysLogRegex, keepParsedGroups );

	    // Import & parse pipe 
	    Pipe importAndParsePipe = new Each( "import", new Fields( "line" ), parser, Fields.RESULTS );
	    // }}
	    
	    
	    // {{
	    // TRANSFORM
	    // Transform the process field - remove process ID if found, for better reporting on logs
	    // Also, convert to lowercase
	    // E.g. Change "ntpd[1302]" to "ntpd" 
	    String expression = "process.substring(0, (process.indexOf('[') == -1 ? process.length()-1 : process.indexOf('[') )).toLowerCase()";
	    Fields fieldProcess = new Fields( "process" );
	    ExpressionFunction expFunc =
	      new ExpressionFunction( fieldProcess, expression, String.class );
	   
	   // Pipe for transformed data
	   Pipe scrubbedDataPipe = new Each( importAndParsePipe, fieldProcess, expFunc, Fields.REPLACE );
	   // }}
	   
	   // {{
	   // REPORT/ANALYZE
	   // Capture counts by process, as a report, sort by count, desc
	   // ------------------------------------------------------------
	   //         process	count()
	   // E.g.    sshd      4
	   Pipe reportPipe = new Pipe("reportByProcess", scrubbedDataPipe);
	   Fields keyFields = new Fields("process");
	   Fields groupByFields = new Fields( "process");
	   Fields countField = new Fields( "countOfEvents" );
	   Fields sortByFields = new Fields( "process");
	   
	   reportPipe = new GroupBy(reportPipe, groupByFields);
	   
	   reportPipe = new Every(reportPipe, keyFields, 
			   new Count(countField), Fields.ALL);
	   
	   reportPipe = new GroupBy(reportPipe,  
			   keyFields,
               countField,
               false); //true=descending order
	   //End of reports
	   //}}

	   // {{
	   // EXECUTE 
	   // Connect the taps, pipes, etc., into a flow & execute
	    FlowDef flowDef = FlowDef.flowDef()
                 .setName( "Log parser" )
                 .addSource( importAndParsePipe, sourceSyslogTap )
                 .addTailSink( scrubbedDataPipe, sinkParsedLogTap )
                 .addTailSink(reportPipe,sinkReportTap)
                 .addTrap( importAndParsePipe, sinkTrapTap );
        Flow flow = flowConnector.connect(flowDef);
        flow.complete();
        // }}
	}

}
