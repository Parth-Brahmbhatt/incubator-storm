/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.hive.bolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.common.HiveOptions;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


public class BucketTestHiveTopology {
    static final String USER_SPOUT_ID = "user-spout";
    static final String BOLT_ID = "my-hive-bolt";
    static final String TOPOLOGY_NAME = "hive-test-topology1";

    public static void main(String[] args) throws Exception {
        if ((args == null) || (args.length < 6)) {
            System.out.println("Usage: BucketTestHiveTopology metastoreURI "
                    + "dbName tableName dataFileLocation hiveBatchSize " +
                    "hiveTickTupl]eIntervalSecs  [topologyNamey] [keytab file]"
                    + " [principal name] ");
            System.exit(1);
        }
        String metaStoreURI = args[0];
        String dbName = args[1];
        String tblName = args[2];
        String sourceFileLocation = args[3];
        Integer hiveBatchSize = Integer.parseInt(args[4]);
        Integer hiveTickTupleIntervalSecs = Integer.parseInt(args[5]);
        String[] colNames = { "s_store_sk", "s_store_id", "s_rec_start_date",
                "s_rec_end_date", "s_closed_date_sk", "s_store_name",
                "s_number_employees", "s_floor_space", "s_hours",
                "s_manager", "s_market_id", "s_geography_class",
                "s_market_desc", "s_market_manager", "s_division_id",
                "s_division_name", "s_company_id", "s_company_name",
                "s_street_number", "s_street_name", "s_street_type",
                "s_suite_number", "s_city", "s_county", "s_state", "s_zip",
                "s_country", "s_gmt_offset", "s_tax_precentage"};
        Config config = new Config();
        config.setNumWorkers(1);
        UserDataSpout spout = new UserDataSpout().withDataFile(sourceFileLocation);
        DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
                .withColumnFields(new Fields(colNames)).withTimeAsPartitionField("YYYY/MM/DD");
        HiveOptions hiveOptions;
        hiveOptions = new HiveOptions(metaStoreURI,dbName,tblName,mapper)
                .withTxnsPerBatch(10)
                .withBatchSize(hiveBatchSize)
                .withIdleTimeout(10);
        // doing below because its affecting storm metrics most likely
        // had to make tick tuple a mandatory argument since its positional
        if (hiveTickTupleIntervalSecs > 0) {
            hiveOptions.withTickTupleInterval(hiveTickTupleIntervalSecs);
        }
        if (args.length == 9) {
            hiveOptions.withKerberosKeytab(args[7]).withKerberosPrincipal(args[8]);
        }
        HiveBolt hiveBolt = new HiveBolt(hiveOptions);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(USER_SPOUT_ID, spout, 1);
        // SentenceSpout --> MyBolt
        builder.setBolt(BOLT_ID, hiveBolt, 14)
                .shuffleGrouping(USER_SPOUT_ID);
        if (args.length == 6) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
            waitForSeconds(20);
            cluster.killTopology(TOPOLOGY_NAME);
            System.out.println("cluster begin to shutdown");
            cluster.shutdown();
            System.out.println("cluster shutdown");
            System.exit(0);
        } else {
            StormSubmitter.submitTopology(args[6], config, builder.createTopology());
        }
    }

    public static void waitForSeconds(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
        }
    }

    public static class UserDataSpout extends BaseRichSpout {
        private ConcurrentHashMap<UUID, Values> pending;
        private SpoutOutputCollector collector;
        private String filePath;
        private BufferedReader br;
        private int count = 0;
        private long total = 0L;

        public UserDataSpout withDataFile (String filePath) {
            this.filePath = filePath;
            return this;
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("s_store_sk", "s_store_id", "s_rec_start_date",
                "s_rec_end_date", "s_closed_date_sk", "s_store_name",
                "s_number_employees", "s_floor_space", "s_hours",
                "s_manager", "s_market_id", "s_geography_class",
                "s_market_desc", "s_market_manager", "s_division_id",
                "s_division_name", "s_company_id", "s_company_name",
                "s_street_number", "s_street_name", "s_street_type",
                "s_suite_number", "s_city", "s_county", "s_state", "s_zip",
                "s_country", "s_gmt_offset", "s_tax_precentage"));
        }

        public void open(Map config, TopologyContext context,
                         SpoutOutputCollector collector) {
            this.collector = collector;
            this.pending = new ConcurrentHashMap<UUID, Values>();
            try {
                this.br = new BufferedReader(new FileReader(new File(this
                        .filePath)));
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        public void nextTuple() {
            String line;
            try {
                if ((line = br.readLine()) != null) {
                    System.out.println("*********" + line);
                    String[] values = line.split("\\|");
                    Values tupleValues = new Values(values);
                    UUID msgId = UUID.randomUUID();
                    this.pending.put(msgId, tupleValues);
                    this.collector.emit(tupleValues, msgId);
                    count++;
                    total++;
                    if (count > 1000) {
                        count = 0;
                        System.out.println("Pending count: " + this.pending.size() + ", total: " + this.total);
                    }
                    Thread.yield();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        public void ack(Object msgId) {
            this.pending.remove(msgId);
        }

        public void fail(Object msgId) {
            System.out.println("**** RESENDING FAILED TUPLE");
            this.collector.emit(this.pending.get(msgId), msgId);
        }
    }
}

