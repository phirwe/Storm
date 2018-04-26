/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package admicloud.storm.pagerank;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;

public class PageRankMain {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new PageRankSpout());
		System.out.println("Finished spout");
		BoltDeclarer fixpoint = builder.setBolt("fixpoint", new PageRankFixPoint());
		BoltDeclarer updater = builder.setBolt("updater", new PageRankUpdate(), 1);
		BoltDeclarer sink = builder.setBolt("sink", new PageRankSink());
		/////////////////////////////////
		fixpoint.globalGrouping("spout");
		/////////////////////////////////
		updater.globalGrouping("fixpoint", "toupdater");
		fixpoint.globalGrouping("updater");
		System.out.println("Finished update");
		/////////////////////////////////
		sink.globalGrouping("fixpoint", "tosink");
		System.out.println("Finished sink");
		
		Config conf = new Config();
		conf.setDebug(false);
		conf.setMaxTaskParallelism(20);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("scc", conf, builder.createTopology());
	}
}
