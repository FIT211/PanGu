
package com.nmlab.pangu.BasicStatistics.Spouts;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jnetpcap.Pcap;
import org.jnetpcap.PcapIf;
import org.jnetpcap.nio.JMemory;
import org.jnetpcap.packet.PcapPacket;
import org.jnetpcap.protocol.tcpip.Tcp;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class PcapSpout4jnetpcap implements IRichSpout {

	private SpoutOutputCollector outputCollector;
	PcapIf device;
	Pcap pcap; 
	StringBuilder errbuf = new StringBuilder(); // For any error msgs
	//start capture
	private String deviceName = null;
	private int count = -1;
	private String filter = null;
	private String srcFilename =null ;
	private String dstFilename = null;
	private int sampLen = 64*1024;
	public int countPacket = 0;
	private int flags = Pcap.MODE_PROMISCUOUS; // capture all packets
	private int timeout = 10 * 1000; // 10 seconds in millis
	
    public PcapSpout4jnetpcap(){};
   
    public PcapSpout4jnetpcap(String deviceName, int count, String filter, String srcFilename, String dstFilename, int sampLen){
    	this.deviceName = deviceName;
    	this.count = count; //鏈娇鐢紝鏃犳晥
    	this.filter = filter;
    	this.srcFilename = srcFilename;
    	this.dstFilename = dstFilename;
    	if(sampLen<0)
    		this.sampLen = 64*1024;
    	this.sampLen = sampLen;
    }
    public PcapSpout4jnetpcap(String deviceName, String count, String filter, String srcFilename, String dstFilename, String sampLen){
    	this.deviceName = deviceName;
    	this.count = Integer.parseInt(count); //鏈娇鐢紝鏃犳晥
    	this.filter = filter;
    	this.srcFilename = srcFilename;
    	this.dstFilename = dstFilename;
    	int slen=0;
    	if(Integer.parseInt(sampLen)<0)
            slen = 64*1024;// Capture all packets, no trucation
    	this.sampLen = slen;

    }

    PcapIf getDevice(){
    	List<PcapIf> alldevs = new ArrayList<PcapIf>(); // Will be filled with NICs
    	int r = Pcap.findAllDevs(alldevs, errbuf);
		if (r == Pcap.NOT_OK || alldevs.isEmpty()) {
			System.err.printf("Can��t read list of devices, error is %s", errbuf.toString());
			return null;
		}
		int i = 0,chooseid=0;
		System.out.printf("Next is NIC list\n");
		for (PcapIf device : alldevs) {
			String description =(device.getDescription() != null) ? device.getDescription(): "No description available";
			if(deviceName!=null && deviceName!="" && device.getName().equals(deviceName))
				chooseid=i;
			System.out.printf("#%d: %s [%s]\n", i++, device.getName(), description);
		}
		System.out.printf("There are total %d nics。We choose %d th nic\n",i,chooseid);
		return alldevs.get(chooseid);
    }

	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector spoutOutputCollector) {
		// TODO Auto-generated method stub
		this.outputCollector = spoutOutputCollector;		
        try {                       
        	if(srcFilename!=null){
        		System.out.println("before open");
        		pcap=Pcap.openOffline(srcFilename, errbuf);
        		if (pcap == null) {
        			System.err.printf("Error while opening srcfile  for capture: "+ errbuf.toString());
        			return;
        		}
				System.out.println("after open");
        	}
        	else
        	{
        		this.sampLen=64*1024;
        		device = getDevice();
        		System.out.println(device);
        		System.out.println(this.sampLen);
        		
        		pcap =Pcap.openLive(device.getName(), this.sampLen, this.flags, this.timeout, errbuf);
        		if (pcap == null) {
        			System.err.printf("Error while opening device for capture: "+ errbuf.toString());
        			return;
        		}
        	}
        	
        } catch (Exception e) {
            e.printStackTrace();
        }
	}

	public void nextTuple() {
		int flag=0;
		// TODO Auto-generated method stub
		final PcapPacket packet = new PcapPacket(JMemory.POINTER);  
		final Tcp tcp = new Tcp(); 
		try {
		
			while(true){				
				pcap.nextEx(packet);
				if(packet==null){
					System.out.println("null");
					break;
				  }
				
				if (packet.hasHeader(Tcp.ID)) {  
					  
                    /* 
                     * Now get our tcp header definition (accessor) peered with actual 
                     * memory that holds the tcp header within the packet. 
                     */  
                    packet.getHeader(tcp);  
  
                    System.out.printf("tcp.dst_port=%d%n", tcp.destination());  
                    System.out.printf("tcp.src_port=%d%n", tcp.source());  
                    System.out.printf("tcp.ack=%x%n", tcp.ack());  
  
                }  
 
				this.outputCollector.emit(createValues(packet));

		       }
			} catch (Exception e) {}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		// TODO Auto-generated method stub
		//outputFieldsDeclarer.declare(new Pcap().createFields());
		outputFieldsDeclarer.declare(new Fields("len"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public Values createValues(PcapPacket pkt) {
		
		/*long sec = pkt.;
		int pro = ip.protocol;
		String src = ip.src_ip.getHostAddress();
		String dst = ip.dst_ip.getHostAddress();*/
		long len = pkt.getTotalSize();
		
        return new Values(
        /*        sec,
                pro,
                src,
                dst,*/
                len
        );
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

}