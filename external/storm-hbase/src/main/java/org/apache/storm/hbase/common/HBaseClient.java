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
package org.apache.storm.hbase.common;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.storm.hbase.bolt.mapper.HBaseProjectionCriteria;
import org.apache.storm.hbase.security.HBaseSecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class HBaseClient {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseClient.class);

    private HTable table;

    private String tableName;

    private Configuration configuration;

    final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
        @Override
        public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
            for (int i = 0; i < e.getNumExceptions(); i++) {
                LOG.info("Failed to sent put " + e.getRow(i) + ".");
            }
        }
    };

    public HBaseClient(Map<String, Object> map , final Configuration configuration, final String tableName) {
        try {
            UserProvider provider = HBaseSecurityUtil.login(map, configuration);
            this.configuration = configuration;
            this.tableName = tableName;

//            this.table = provider.getCurrent().getUGI().doAs(new PrivilegedExceptionAction<HTable>() {
//                @Override
//                public HTable run() throws IOException {
//                    return new HTable(configuration, tableName);
//                }
//            });
        } catch(Exception e) {
            throw new RuntimeException("HBase bolt preparation failed: " + e.getMessage(), e);
        }
    }

    public List<Mutation> constructMutationReq(byte[] rowKey, ColumnList cols, Durability durability) {
        List<Mutation> mutations = Lists.newArrayList();

        if (cols.hasColumns()) {
            Put put = new Put(rowKey);
            //put.setDurability(durability);
            for (ColumnList.Column col : cols.getColumns()) {
                if (col.getTs() > 0) {
                    put.add(
                            col.getFamily(),
                            col.getQualifier(),
                            col.getTs(),
                            col.getValue()
                    );
                } else {
                    put.add(
                            col.getFamily(),
                            col.getQualifier(),
                            col.getValue()
                    );
                }
            }
            mutations.add(put);
        }

        if (cols.hasCounters()) {
            Increment inc = new Increment(rowKey);
            inc.setDurability(durability);
            for (ColumnList.Counter cnt : cols.getCounters()) {
                inc.addColumn(
                        cnt.getFamily(),
                        cnt.getQualifier(),
                        cnt.getIncrement()
                );
            }
            mutations.add(inc);
        }

        if (mutations.isEmpty()) {
            mutations.add(new Put(rowKey));
        }

        return mutations;
    }

    public void batchMutate(List<Mutation> mutations) throws Exception {
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName))
                .listener(listener);
        try (Connection connection = ConnectionFactory.createConnection(configuration);
             final BufferedMutator mutator = connection.getBufferedMutator(params)) {
            for (Mutation message : mutations) {
                mutator.mutate(message);
            }
        } catch (IOException ex) {
            LOG.warn("Error performing a mutation to HBase.", ex);
        }
    }


    public Get constructGetRequests(byte[] rowKey, HBaseProjectionCriteria projectionCriteria) {
        Get get = new Get(rowKey);

        if (projectionCriteria != null) {
            for (byte[] columnFamily : projectionCriteria.getColumnFamilies()) {
                get.addFamily(columnFamily);
            }

            for (HBaseProjectionCriteria.ColumnMetaData columnMetaData : projectionCriteria.getColumns()) {
                get.addColumn(columnMetaData.getColumnFamily(), columnMetaData.getQualifier());
            }
        }

        return get;
    }

    public Result[] batchGet(List<Get> gets) throws Exception {
        try {
            return table.get(gets);
        } catch (Exception e) {
            LOG.warn("Could not perform HBASE lookup.", e);
            throw e;
        }
    }
}
