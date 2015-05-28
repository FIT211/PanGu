package com.nmlab.pangu.BasicStatistics.Bolts;

import java.io.BufferedOutputStream;

import jpcap.packet.IPPacket;

import java.math.BigInteger; 
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class throughputBolt implements IRichBolt{
	
	private OutputCollector outputCollector;
	public long time = 0;
	public long throughput = 0;
	public long countPacket = 0;
	//FileWriter fw = null;
    
	//private FileWriter fw= null;


	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		try {
			//fw = new FileWriter("D:\\开发工具\\eclipse-java-luna-SR2-win32-x86_64\\workspace\\pcapStorm\\throughput.txt",true);
			countPacket++;
			if(time == 0) 
				time = (Long) tuple.getValueByField("sec");
			if((Long) tuple.getValueByField("sec") - time >= 0)
			{
				throughput = throughput + (Long) tuple.getValueByField("len");
				System.out.println(time+"   "+throughput+"   "+countPacket);
				//fw.write(Long.toString(time)+"    "+Long.toString(throughput)+"    "+Long.toString(countPacket)+"\r\n");   
				//this.outputCollector.emit(tuple, tuple.getValues());
				this.outputCollector.emit(new Values(time,throughput,countPacket));
				
				//关键变量： time代表当前的时间片，throughput代表该时间片的吞吐量，单位byte,countPacket代表分组数，
				//接下来这些变量都会被重置，进入下一个时间片，请在这里进行操作
				throughput = 0;
				countPacket = 0;
				time = (Long) tuple.getValueByField("sec");
			}
			else throughput = throughput + (Long) tuple.getValueByField("len");
			//fw.close();
			
            
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
		outputFieldsDeclarer.declare(new Fields("timestamp","throughput","countPacket"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	  public static String binary(Object object, int radix){  
	        return new BigInteger(1, (byte[]) object).toString(radix);// 这里的1代表正数  
	}  

}
