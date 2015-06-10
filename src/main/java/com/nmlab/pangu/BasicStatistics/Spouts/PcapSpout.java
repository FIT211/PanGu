package com.nmlab.pangu.BasicStatistics.Spouts;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.Map;

import com.nmlab.pangu.BasicStatistics.Helpers.Application;
import com.nmlab.pangu.BasicStatistics.Helpers.Http;
import com.nmlab.pangu.BasicStatistics.Helpers.PacketCapturer;
import com.nmlab.pangu.BasicStatistics.Helpers.Pcap;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import jpcap.NetworkInterface;
import jpcap.NetworkInterfaceAddress;
import jpcap.packet.IPPacket;
import jpcap.packet.TCPPacket;
import jpcap.packet.UDPPacket;
import jpcap.packet.Packet;
import jpcap.JpcapCaptor;
import jpcap.PacketReceiver;
public class PcapSpout implements IRichSpout {

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
	
	private Application application;
	private Http[] http;
	private int httptypecount=0;
    public PcapSpout(){};
    public Writer writer;
    public DataOutputStream fos;
    public PcapSpout(String deviceName, int count, String filter, String srcFilename, String dstFilename, int sampLen){
    	this.deviceName = deviceName;
    	this.count = count; //鏈娇鐢紝鏃犳晥
    	this.filter = filter;
    	this.srcFilename = srcFilename;
    	this.dstFilename = dstFilename;
    	if(sampLen<0)
    		this.sampLen = 65535;
    	this.sampLen = sampLen;
    }
    public PcapSpout(String deviceName, String count, String filter, String srcFilename, String dstFilename, String sampLen){
    	this.deviceName = deviceName;
    	this.count = Integer.parseInt(count); //鏈娇鐢紝鏃犳晥
    	this.filter = filter;
    	this.srcFilename = srcFilename;
    	this.dstFilename = dstFilename;
    	int slen=0;
    	if(Integer.parseInt(sampLen)<0)
            slen = 65535;
    	this.sampLen = slen;

    }
    
    /**
	 * 
	 * @return
	 */
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
		return devices[4];
	}
    
    
    public class PacketPrinter implements PacketReceiver {
    	
    	public void receivePacket(Packet packet) {
    		//this.outputCollector.emit(createValues(packet));
    	}
    }
    
	public void ack(Object arg0) {
		// TODO Auto-generated method stub
		
	}

	public void activate() {
		// TODO Auto-generated method stub
		
	}

	public void close() {
		// TODO Auto-generated method stub
		
	}

	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	public void fail(Object arg0) {
		// TODO Auto-generated method stub
		
	}

	public void nextTuple() {
		int flag=0;
		// TODO Auto-generated method stub
		try {
			if(sampLen<0) sampLen = 65535;		
			while(true){
				Packet packet=captor.getPacket();  
				//if some error occurred or EOF has reached, break the loop  
				if(packet==null){
					//System.out.println("null");
					break;
				  }
				  //otherwise, print out the packet
				  IPPacket ip = (IPPacket)packet;
		          if(packet instanceof TCPPacket)
		          {
		        	  TCPPacket tcp=(TCPPacket)packet;
		        	  this.outputCollector.emit(createValues(ip,tcp));
		        	  break;
		          }
		          else if(packet instanceof UDPPacket)
		          {
		        	  UDPPacket udp=(UDPPacket)packet;
		        	  this.outputCollector.emit(createValues(ip,udp));
		        	  break;
		          }
		          else
		          {
		        	  this.outputCollector.emit(createValues(ip));
		          }
		       }
			} catch (Exception e) {}
	}

	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector spoutOutputCollector) {
		// TODO Auto-generated method stub
		this.outputCollector = spoutOutputCollector;
		this.application=new Application();
		this.http=new Http[1000];
		try {
			this.fos =new DataOutputStream(new FileOutputStream("10_29.txt"));
		} catch (FileNotFoundException e1) {
			// TODO 鑷姩鐢熸垚鐨� catch 鍧�
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO 鑷姩鐢熸垚鐨� catch 鍧�
			e.printStackTrace();
		}
		
		/*for(int i=0;i<1000;i++)
		{
			this.http[i]=new Http();
		}*/
		//System.out.println("http[0].length:"+this.http[0].Getlength());
		//this.http=new Http[1000];
        try {            
            //璇诲彇HDFS鏂囦欢鐨勫鎴风锛岃嚜宸卞疄鐜�
        	
        	/*
        	in = new BufferedReader(new FileReader("D:\\1.txt"));
        	if(in == null){
        		System.out.println("鏂囦欢鎵撳紑澶辫触");
        		System.exit(-1);
        	}*/
        	//pc = new PacketCapturer();
        //	captor = new JpcapCaptor();
        	if(srcFilename!=null){
        		System.out.println("before open");
				captor = JpcapCaptor.openFile(srcFilename);
				System.out.println("after open");
        	}
        	else
        	{
        		getNetworkInterfaces();
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
		//outputFieldsDeclarer.declare(new Pcap().createFields());
		outputFieldsDeclarer.declare(new Pcap().createFields());
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public Values createValues(IPPacket ip,TCPPacket tcp) {
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
                len,
                tcp
        );
	}
	public Values createValues(IPPacket ip,UDPPacket udp) {
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
                len,
                udp
        );
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
                    len,
                    ip
            );
    }

}