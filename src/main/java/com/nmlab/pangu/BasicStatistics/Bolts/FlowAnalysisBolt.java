package com.nmlab.pangu.BasicStatistics.Bolts;

import java.io.BufferedOutputStream;
import com.nmlab.pangu.BasicStatistics.Helpers.Flow;
import com.nmlab.pangu.BasicStatistics.Helpers.Pcap;
import jpcap.packet.IPPacket;

import java.math.BigInteger; 
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class FlowAnalysisBolt implements IRichBolt{
	
	private OutputCollector outputCollector;
	public long time = 0;
	public long throughput = 0;
	public long countPacket = 0;
	public String src;
	public String dst;
	ArrayList<Flow> flowArray = new ArrayList<Flow>();
	
	int i=0;
	FileWriter fw = null;
    
	//private FileWriter fw= null;


	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		try {
			fw = new FileWriter("D:\\开发工具\\eclipse-java-luna-SR2-win32-x86_64\\workspace\\pcapStorm\\throughput.txt",true);
			countPacket++;
			if(time == 0) 
				time = (Long) tuple.getValueByField("sec");
			if((Long) tuple.getValueByField("sec") - time == 1)
			{
				time = (Long) tuple.getValueByField("sec");
			}
			else {
				
			}
			fw.close();
			this.outputCollector.emit(tuple, tuple.getValues());
            
        } catch (Exception e) {
            
        } finally {
            outputCollector.ack(tuple);
        }
	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector outputCollector) {
		// TODO Auto-generated method stub
		this.outputCollector = outputCollector;
        try {
            //初始化HBase数据库
        	//fw = new FileWriter("D:\\1-test.txt");
        	

        } catch (Exception e) {
           
        }
	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		// TODO Auto-generated method stub
		outputFieldsDeclarer.declare(new Pcap().createFields());
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	  public static String binary(Object object, int radix){  
	        return new BigInteger(1, (byte[]) object).toString(radix);// 这里的1代表正数  
	}  

}