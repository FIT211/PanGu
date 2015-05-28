package com.nmlab.pangu.BasicStatistics.Spouts;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;

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
	
    public PcapSpout(){};
   
    public PcapSpout(String deviceName, int count, String filter, String srcFilename, String dstFilename, int sampLen){
    	this.deviceName = deviceName;
    	this.count = count; //未使用，无效
    	this.filter = filter;
    	this.srcFilename = srcFilename;
    	this.dstFilename = dstFilename;
    	if(sampLen<0)
    		this.sampLen = 65535;
    	this.sampLen = sampLen;
    }
    public PcapSpout(String deviceName, String count, String filter, String srcFilename, String dstFilename, String sampLen){
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
		return null;
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
		// TODO Auto-generated method stub
		//Utils.sleep(10000);//变慢的罪魁祸首
		try {
            //该方法从指定的目录中中读取符合条件的文件列表，并随机从中选择一个将其独占并返回文件名
			/*
			if( in != null){
				String line;
				int count=0;
				while((line = in.readLine()) != null){
					if(line.trim().length()>0){
						count++;
						this.outputCollector.emit(createValues(line));
					}
				}
				System.out.println("There are "+count+" rows!");
			}
			else{
				System.out.println("文件打开失败 in next tuple");
			}
			
			*/
			if(sampLen<0) sampLen = 65535;
						
				while(true){
				  Packet packet=captor.getPacket();  
				  //if some error occurred or EOF has reached, break the loop  
				  if(packet==null){System.out.println("null");break;}
				  //otherwise, print out the packet
				    IPPacket ip = (IPPacket)packet;
				    countPacket++;
				    //System.out.println(countPacket++);
		            /*String protocol = null;
		            switch(new Integer(ip.protocol))
		            {
		            case 1:protocol = "ICMP";break;
		            case 2:protocol = "IGMP";break;
		            case 6:protocol = "TCP";break;
		            case 8:protocol = "EGP";break;
		            case 9:protocol = "IGP";break;
		            case 17:protocol = "UDP";break;
		            case 41:protocol = "IPv6";break;
		            case 89:protocol = "OSPF";break;
		            default:break;
		            }
		            
		            System.out.print("timestamp:"+ip.sec+ "  ");
		            System.out.print("protocal:" + protocol + "  ");
		            System.out.print("src IP:" + ip.src_ip.getHostAddress() + "  ");
		            System.out.print("dst IP:" + ip.dst_ip.getHostAddress() + "  ");
		            System.out.println("length:" + ip.length + "  ");*/
		            
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
            //读取HDFS文件的客户端，自己实现
        	
        	/*
        	in = new BufferedReader(new FileReader("D:\\1.txt"));
        	if(in == null){
        		System.out.println("文件打开失败");
        		System.exit(-1);
        	}*/
        	//pc = new PacketCapturer();
        //	captor = new JpcapCaptor();
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
		//outputFieldsDeclarer.declare(new Pcap().createFields());
		outputFieldsDeclarer.declare(new Pcap().createFields());
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
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

}
