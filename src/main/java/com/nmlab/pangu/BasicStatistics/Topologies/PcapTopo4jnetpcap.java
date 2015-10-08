
package com.nmlab.pangu.BasicStatistics.Topologies;
import  com.nmlab.pangu.BasicStatistics.Spouts.*;
import  com.nmlab.pangu.BasicStatistics.Bolts.*;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import backtype.storm.StormSubmitter;

public class PcapTopo4jnetpcap {
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		 Config conf = new Config();
		 if (args == null || args.length == 0) {
			 conf.put("storm.zookeeper.port", 2000);			 
			 builder.setSpout("PcapSpout4jnetpcap", new PcapSpout4jnetpcap(null,-1,null,null,null,-1), 1);
			 builder.setBolt("expbolt4jnetpcap", new expbolt4jentpcap(),1).shuffleGrouping("PcapSpout4jnetpcap");
		
			 conf.setNumWorkers(1);
			 LocalCluster cluster = new LocalCluster();
			 cluster.submitTopology("PcapTopo4jnetpcap", conf, builder.createTopology()); 
		     Utils.sleep(1000000);
			 cluster.killTopology("PcapTopo4jnetpcap");
			 cluster.shutdown();
		 }
		 else{
			 builder.setSpout("PcapSpout4jnetpcap", new PcapSpout4jnetpcap(null,-1,null,null,null,-1), 1);
			 builder.setBolt("expbolt4jnetpcap", new expbolt4jentpcap(),1).shuffleGrouping("PcapSpout4jnetpcap");
			
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
