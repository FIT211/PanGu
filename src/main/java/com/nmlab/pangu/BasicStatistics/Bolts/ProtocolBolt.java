package com.nmlab.pangu.BasicStatistics.Bolts;

import java.io.BufferedOutputStream;
import com.nmlab.pangu.BasicStatistics.Helpers.Pcap;
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
import backtype.storm.tuple.Tuple;

public class ProtocolBolt implements IRichBolt{
	
	private OutputCollector outputCollector;
	public long time = 0;
	public int pro = 0;
	public String protocol = null;
	public long tcpCount = 0;
	public long udpCount = 0;
	public long icmpCount = 0;
	public long igmpCount = 0;
	public long tcpLen = 0;
	public long udpLen = 0;
	public long icmpLen = 0;
	public long igmpLen = 0;
	FileWriter fw = null;
    
	//private FileWriter fw= null;


	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		try {
			fw = new FileWriter("D:\\开发工具\\eclipse-java-luna-SR2-win32-x86_64\\workspace\\pcapStorm\\protocal.txt",true);
			if(time == 0) 
				time = (Long) tuple.getValueByField("sec");
			if((Long) tuple.getValueByField("sec") - time == 1)
			{
				pro = (Integer) tuple.getValueByField("pro");
				//System.out.println("pro"+pro);
				switch(new Integer(pro))
		        {
		            case 1:protocol = "ICMP";icmpCount++;icmpLen+=(Long) tuple.getValueByField("len");break;
		            case 2:protocol = "IGMP";igmpCount++;igmpLen+=(Long) tuple.getValueByField("len");break;
		            case 6:protocol = "TCP";tcpCount++;tcpLen+=(Long) tuple.getValueByField("len");break;
		            case 8:protocol = "EGP";break;
		            case 9:protocol = "IGP";break;
		            case 17:protocol = "UDP";udpCount++;udpLen+=(Long) tuple.getValueByField("len");break;
		            case 41:protocol = "IPv6";break;
		            case 89:protocol = "OSPF";break;
		            default:break;
		         }
				fw.write("time:"+time+"	TCP:"+tcpCount+"	"+tcpLen+"	UDP:"+udpCount+"	"+udpLen+"	ICMP:"+icmpCount+"	"+icmpLen+"\r\n");
				//关键变量： time代表当前的时间片，xxCount代表协议分组数，xxLen代表协议吞吐量，单位byte
				//接下来这些变量都会被重置，进入下一个时间片，请在这里进行操作
				tcpCount = 0;
				tcpLen = 0;
				time = (Long) tuple.getValueByField("sec");
			}
			else {
				pro = (Integer) tuple.getValueByField("pro");
				//System.out.println("pro"+pro);
				switch(new Integer(pro))
		        {
		            case 1:protocol = "ICMP";icmpCount++;icmpLen+=(Long) tuple.getValueByField("len");break;
		            case 2:protocol = "IGMP";break;
		            case 6:protocol = "TCP";tcpCount++;tcpLen+=(Long) tuple.getValueByField("len");break;
		            case 8:protocol = "EGP";break;
		            case 9:protocol = "IGP";break;
		            case 17:protocol = "UDP";udpCount++;udpLen+=(Long) tuple.getValueByField("len");break;
		            case 41:protocol = "IPv6";break;
		            case 89:protocol = "OSPF";break;
		            default:break;
		        }
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

