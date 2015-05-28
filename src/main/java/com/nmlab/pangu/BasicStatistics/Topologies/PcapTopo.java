package com.nmlab.pangu.BasicStatistics.Topologies;
import com.nmlab.pangu.BasicStatistics.Spouts.*;
import com.nmlab.pangu.BasicStatistics.Bolts.*;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import backtype.storm.StormSubmitter;

public class PcapTopo {
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		 Config conf = new Config();
		 if (args == null || args.length == 0) {
			 conf.put("storm.zookeeper.port", 2000);
			// builder.setSpout("PcapSpout", new PcapSpout(null,-1,null,"E:\\data0.pcap",null,-1), 1);
			 builder.setSpout("PcapSpout", new PcapSpout(null,-1,null,null,null,-1), 1);
			 builder.setBolt("BasicThroughputBolt", new BasicThoughputBolt(),1).shuffleGrouping("PcapSpout");
			 builder.setBolt("PcapRedisBolt",new PcapRedisBolt(),1).shuffleGrouping("BasicThroughputBolt");
			// builder.setBolt("ProtocolBolt", new ProtocolBolt(),1).shuffleGrouping("PcapSpout");
			 conf.setNumWorkers(1);
			 LocalCluster cluster = new LocalCluster();  
		       
		        cluster.submitTopology("PcapTopo", conf, builder.createTopology());  
		        System.out.println();
		        Utils.sleep(1000000);  
		        cluster.killTopology("PcapTopo");  
		        cluster.shutdown();
		 }
		 else{
			 System.out.println(args[0]);
        	 /*System.out.println(args[1]);
        	 System.out.println(args[2]);
        	 System.out.println(args[3]);
        	 System.out.println(args[4]);
        	 System.out.println(args[5]);
        	 System.out.println(args[6]);
        	 builder.setSpout("PcapSpout", new PcapSpout(args[1],args[2],args[3],args[4],args[5],args[6]), 2);
        	 */
        	 builder.setSpout("PcapSpout", new PcapSpout(null,-1,null,null,null,65535), 1);
        	// builder.setSpout("PcapSpout", new PcapSpout(null,-1,null,"D:\\PROJECTS\\EclipseWorkstation\\01\\data0.pcap",null,-1), 1);
        	 builder.setBolt("BasicThoughputBolt", new BasicThoughputBolt(),1).shuffleGrouping("PcapSpout");
        	 builder.setBolt("PcapRedisBolt",new PcapRedisBolt(),1).shuffleGrouping("BasicThoughputBolt");
     	    //builder.setBolt("IpAnalysisBolt", new IpAnalysisBolt(),1).shuffleGrouping("PcapSpout");
     		//builder.setBolt("ProtocolBolt", new ProtocolBolt(),1).shuffleGrouping("PcapSpout");
     		//builder.setBolt("FlowAnalysisBolt", new ProtocolBolt(),1).shuffleGrouping("PcapSpout");
        	 conf.setNumWorkers(1);     
        	 try{
        		 StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        	 }catch (InvalidTopologyException e ){
        		 e.printStackTrace();
        	 } catch (AlreadyAliveException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	 
		 }
	   
        
	}
}
