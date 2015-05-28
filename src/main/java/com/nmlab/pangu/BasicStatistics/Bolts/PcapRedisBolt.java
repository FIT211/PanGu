package com.nmlab.pangu.BasicStatistics.Bolts;

import java.util.Map;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class PcapRedisBolt implements IBasicBolt{

	private BasicOutputCollector outputCollector;
	int count=0;
	JedisPool pool;
	Jedis jedis;
	String result,result_countPac;

	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		this.pool = new JedisPool(new JedisPoolConfig(), "166.111.143.245");
		jedis = pool.getResource();
		result="";
		result_countPac="";
		count=0;
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		outputCollector = collector;
		outputCollector.setContext(input); 
		try {
			String throughput = Long.toString((Long)input.getValueByField("throughput")) ;
			String countPacket = Long.toString((Long) input.getValueByField("countPacket"));
	    	//System.out.println("timestamp:"+input.getValueByField("timestamp")+" thoughput: "+input.getValueByField("throughput")+ " countPacket:" +input.getValueByField("countPacket")) ;
			//fw.write("timestamp:"+tuple.getValue(0)+" flow: "+tuple.getValue(1));
	    	count++;
	    	if(count<10){
	    		result+="/"+throughput;
	    		result_countPac+="/"+countPacket;
	    	}else{
	    		int secPos = result.indexOf("/", 1);
	    		result = result.substring(secPos);
	    		result+="/"+throughput;
	    		
	    		secPos = result_countPac.indexOf("/", 1);
	    		result_countPac = result_countPac.substring(secPos);
	    		result_countPac+="/"+countPacket;
	    	}
	    	
	    	jedis.set("throuput", result);
	    	//System.out.println(jedis.get("timestamp"));
	    	
	    	//System.out.println(result);
	    	
	    } catch(FailedException e) {  
	    	System.out.println("fail to deal with packet");
	    }  
	}

	public void cleanup() {
		// TODO Auto-generated method stub
		 if (jedis != null) {
	    	    jedis.close();
	    	  }
	    
	    pool.destroy();
	}

}
