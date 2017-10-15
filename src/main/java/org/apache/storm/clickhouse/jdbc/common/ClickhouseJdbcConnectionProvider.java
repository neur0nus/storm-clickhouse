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
package org.apache.storm.clickhouse.jdbc.common;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.jdbc.common.ConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClickhouseJdbcConnectionProvider implements ConnectionProvider {

	private static final long serialVersionUID = 144657438517204620L;
	private static final Logger LOG = LoggerFactory.getLogger(ClickhouseJdbcConnectionProvider.class);
	
	private final Map<String, Object> hikariConfigMap;
	private transient HikariCPConnectionProviderExt delegate;
	
	public ClickhouseJdbcConnectionProvider(Map<String, Object> configMap) {
		this.hikariConfigMap = new HashMap<String, Object>();
		if (configMap!=null) this.hikariConfigMap.putAll(configMap);
		this.hikariConfigMap.put("dataSourceClassName", ClickhouseJdbcDataSourceWrapper.class.getName());
		this.delegate = new HikariCPConnectionProviderExt(this.hikariConfigMap);
	}
	
	@Override
	public void prepare() {
		this.delegate.prepare();
		LOG.info("ClickHouse connection provider initiated");
	}

	@Override
	public Connection getConnection() {
		return this.delegate.getConnection();
	}

	@Override
	public void cleanup() {
		this.delegate.cleanup();
		this.delegate = null;
		LOG.info("ClickHouse connection provider cleaned up");
	}

}
