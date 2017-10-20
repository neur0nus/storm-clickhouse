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
package org.apache.storm.clickhouse.bolt;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.clickhouse.jdbc.common.ClickhouseJdbcConnectionProvider;
import org.apache.storm.clickhouse.jdbc.common.JsonJdbcMapper;
import org.apache.storm.jdbc.bolt.AbstractJdbcBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClickhouseInsertBolt extends AbstractJdbcBolt {
	private static final long serialVersionUID = 5457869530320532963L;
	private static final Logger LOG = LoggerFactory.getLogger(ClickhouseInsertBolt.class);

    private String tableName;
    private String insertQuery;
    private transient JdbcMapper jdbcMapper;

	public ClickhouseInsertBolt(Map<String, Object> configMap) {
		super(new ClickhouseJdbcConnectionProvider(configMap));
	}
    
	public ClickhouseInsertBolt(Map<String, Object> configMap, JdbcMapper jdbcMapper) {
		super(new ClickhouseJdbcConnectionProvider(configMap));
        Validate.notNull(jdbcMapper);
        this.jdbcMapper = jdbcMapper;
	}
	
    public ClickhouseInsertBolt(ConnectionProvider connectionProvider) {
        super(connectionProvider);
    }
    
    public ClickhouseInsertBolt(ConnectionProvider connectionProvider,  JdbcMapper jdbcMapper) {
        super(connectionProvider);
        Validate.notNull(jdbcMapper);
        this.jdbcMapper = jdbcMapper;
    }
    
    public String getTableName() {
		return tableName;
	}

	public String getInsertQuery() {
		return insertQuery;
	}

	public JdbcMapper getJdbcMapper() {
		return jdbcMapper;
	}

	public ClickhouseInsertBolt withTableName(String tableName) {
        if (insertQuery != null) {
            throw new IllegalArgumentException("You can not specify both insertQuery and tableName.");
        }
        this.tableName = tableName;
        return this;
    }

    public ClickhouseInsertBolt withInsertQuery(String insertQuery) {
        if (this.tableName != null) {
            throw new IllegalArgumentException("You can not specify both insertQuery and tableName.");
        }
        this.insertQuery = insertQuery;
        return this;
    }

    public ClickhouseInsertBolt withQueryTimeoutSecs(int queryTimeoutSecs) {
        this.queryTimeoutSecs = queryTimeoutSecs;
        return this;
    }

    @SuppressWarnings("rawtypes")
	@Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        super.prepare(map, topologyContext, collector);
        if (this.jdbcMapper==null) this.jdbcMapper = new JsonJdbcMapper();
        if(StringUtils.isBlank(tableName) && StringUtils.isBlank(insertQuery)) {
            throw new IllegalArgumentException("You must supply either a tableName or an insert Query.");
        }
        if (this.jdbcMapper instanceof JsonJdbcMapper) {
        	JsonJdbcMapper ref = (JsonJdbcMapper)this.jdbcMapper;
        	Connection conn = null; ResultSet rs = null;
        	try {
        		conn = super.connectionProvider.getConnection();
        		DatabaseMetaData dbMeta = conn.getMetaData();
        		rs = dbMeta.getTables(null, null, null, new String[]{"TABLE"});
        		if (!rs.next()) throw new IllegalStateException("Can't get tables list from ClickHouse database");
        		String actualTableName = rs.getString("TABLE_NAME");
        		rs.close();
				rs = dbMeta.getColumns(null, null, actualTableName, null);
				LOG.info("Found table '{}'", actualTableName);
				Map<String, String> columnsTypeMap = new HashMap<String, String>();
				while (rs.next()) {
					String columnName = rs.getString("COLUMN_NAME");
					String columnType = rs.getString("TYPE_NAME");
					columnsTypeMap.put(columnName, columnType);
					LOG.info(" resolved '{}' column: '{}' with type {}", actualTableName, columnName, columnType);
				}
				ref.initializeColumns(columnsTypeMap);
        	} catch(Exception err) {
        		LOG.error("JSON JDBC mapper initialization error", err);
        		throw new IllegalStateException(err);
        	} finally {
        		if (rs!=null) try { rs.close(); } catch(Exception err) { LOG.warn("ResultSet closing error", err); }
        		if (conn!=null) try { conn.close(); } catch(Exception err) { LOG.warn("Connection closing error", err); }
        	}
        }
        LOG.info("ClickHouse insert bolt prepared");
    }

    @SuppressWarnings("rawtypes")
	@Override
    protected void process(Tuple tuple) {
        try {
            List<Column> columns = jdbcMapper.getColumns(tuple);
            List<List<Column>> columnLists = new ArrayList<List<Column>>();
            columnLists.add(columns);
            if(!StringUtils.isBlank(tableName)) {
                this.jdbcClient.insert(this.tableName, columnLists);
            } else {
                this.jdbcClient.executeInsertQuery(this.insertQuery, columnLists);
            }
            this.collector.ack(tuple);
        } catch (Exception e) {
            this.collector.reportError(e);
            this.collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

}
