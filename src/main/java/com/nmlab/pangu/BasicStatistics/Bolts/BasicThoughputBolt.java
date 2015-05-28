package com.nmlab.pangu.BasicStatistics.Bolts;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BasicThoughputBolt implements IBasicBolt{

	private BasicOutputCollector outputCollector;
	public long time = 0;
	public long throughput = 0;
	public long countPacket = 0;
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("timestamp","throughput","countPacket"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub

	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		outputCollector = collector;
		outputCollector.setContext(tuple);  
		try {
			//fw = new FileWriter("D:\\开发工具\\eclipse-java-luna-SR2-win32-x86_64\\workspace\\pcapStorm\\throughput.txt",true);
			countPacket++;
			if(time == 0) 
				time = (Long) tuple.getValueByField("sec");
			if((Long) tuple.getValueByField("sec") - time >= 1)
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
			
            
        } catch(FailedException e) {
 
	    	System.out.println("fail to deal with packet");
	    }  

	}

	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

}
