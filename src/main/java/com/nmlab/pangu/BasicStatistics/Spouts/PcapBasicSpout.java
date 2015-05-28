package com.nmlab.pangu.BasicStatistics.Spouts;

import java.util.Map;

import com.nmlab.pangu.BasicStatistics.Helpers.Pcap;

import jpcap.JpcapCaptor;
import jpcap.NetworkInterface;
import jpcap.NetworkInterfaceAddress;
import jpcap.packet.IPPacket;
import jpcap.packet.Packet;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Values;


public class PcapBasicSpout extends BaseRichSpout {
	
	private SpoutOutputCollector outputCollector;
	private JpcapCaptor captor;
	private NetworkInterface device;
	
	//start capture
	private String deviceName = null;
	private int count = -1;
	private String filter = null;
	private String srcFilename =null ;
	private String dstFilename = null;
	private int sampLen = 65535;
	public int countPacket = 0;
	
    public PcapBasicSpout(){};
   
    public PcapBasicSpout(String deviceName, int count, String filter, String srcFilename, String dstFilename, int sampLen){
    	this.deviceName = deviceName;
    	this.count = count; //未使用，无效
    	this.filter = filter;
    	this.srcFilename = srcFilename;
    	this.dstFilename = dstFilename;
    	if(sampLen<0)
    		this.sampLen = 65535;
    	this.sampLen = sampLen;
    }
    public PcapBasicSpout(String deviceName, String count, String filter, String srcFilename, String dstFilename, String sampLen){
    	this.deviceName = deviceName;
    	this.count = Integer.parseInt(count); //未使用，无效
    	this.filter = filter;
    	this.srcFilename = srcFilename;
    	this.dstFilename = dstFilename;
    	int slen=0;
    	if(Integer.parseInt(sampLen)<0)
            slen = 65535;
    	this.sampLen = slen;
    	
    }

	public void nextTuple() {
		// TODO Auto-generated method stub
		try {
           
			if(sampLen<0) sampLen = 65535;
						
				while(true){
				  Packet packet=captor.getPacket();  
				  //if some error occurred or EOF has reached, break the loop  
				  if(packet==null){System.out.println("null");break;}
				  //otherwise, print out the packet
				    IPPacket ip = (IPPacket)packet;
				    countPacket++;
				    
		            
					this.outputCollector.emit(createValues(ip));
					//this.outputCollector.ack(tuple);
				}
				
			
        } catch (Exception e) {
            
        }
	}

	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector spoutOutputCollector) {
		// TODO Auto-generated method stub
		this.outputCollector = spoutOutputCollector;
        try {            
            
        	if(srcFilename!=null){
				captor = JpcapCaptor.openFile(srcFilename);
        	}
        	else
        	{
        		
        		device = getDevice(deviceName);
        		System.out.println(device);
				captor = JpcapCaptor.openDevice(device, sampLen, false, 20);
				if(filter!= null)
					captor.setFilter(filter, true);
        	}
        	
        } catch (Exception e) {
            e.printStackTrace();
        }
	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		// TODO Auto-generated method stub
		outputFieldsDeclarer.declare(new Pcap().createFields());
	}
	public Values createValues(IPPacket ip) {
		long sec = ip.sec;
		int pro = ip.protocol;
		String src = ip.src_ip.getHostAddress();
		String dst = ip.dst_ip.getHostAddress();
		long len = ip.length;
		
        return new Values(
                sec,
                pro,
                src,
                dst,
                len
        );
    }
	static NetworkInterface[] getNetworkInterfaces()
	{
		//Obtain the list of network interfaces
		NetworkInterface[] devices = JpcapCaptor.getDeviceList();
		
		//for each network interface
		for (int i = 0; i < devices.length; i++) {  
			System.out.println(i+": "+devices[i].name + "(" + devices[i].description+")");   
			System.out.println(" datalink: "+devices[i].datalink_name + "(" + devices[i].datalink_description+")");  
			System.out.print(" MAC address:");  
			
			for (byte b : devices[i].mac_address)    
				System.out.print(Integer.toHexString(b&0xff) + ":");  
			System.out.println();  
			
			//print out its IP address, subnet mask and broadcast address  
			for (NetworkInterfaceAddress a : devices[i].addresses)    
				System.out.println(" address:"+a.address + " " + a.subnet + " "+ a.broadcast);
		}		
		return devices;
	}
    
    NetworkInterface getDevice(String deviceName){
		NetworkInterface[] devices = getNetworkInterfaces();
		
		if(deviceName==null) return devices[0];
		
		for(int i=0;i<devices.length; i++){
			if(devices[i].name.equals(deviceName))
				return devices[i];
		}
		return null;
	}
    
    
}
