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

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import javax.sql.DataSource;

import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

public class ClickhouseJdbcDataSourceWrapper implements DataSource {

	private final ClickHouseProperties props;
	
	private BalancedClickhouseDataSource delegate;
	private String url;
	private PrintWriter logWriter;
	private int loginTimeout;
	private Logger parentLogger;
	
	public ClickhouseJdbcDataSourceWrapper() {
		this.props = new ClickHouseProperties();
	}
	
	//// Methods for set roperties
	public void setUrl(String val) {
		this.url = val;
	}
    public void setProfile(String profile) {
        this.props.setProfile(profile);
    }
    public void setCompress(String isCompress) {
        this.props.setCompress(Boolean.parseBoolean(isCompress));
    }
    public void setDecompress(String isDecompress) {
        this.props.setDecompress(Boolean.parseBoolean(isDecompress));
    }
    public void setAsync(String isAsync) {
        this.props.setAsync(Boolean.parseBoolean(isAsync));
    }
    public void setMaxThreads(String maxThreads) {
        this.props.setMaxThreads(Integer.parseInt(maxThreads));
    }
    public void setMaxBlockSize(String maxBlockSize) {
        this.props.setMaxBlockSize(Integer.parseInt(maxBlockSize));
    }
    public void setBufferSize(String bufferSize) {
        this.props.setBufferSize(Integer.parseInt(bufferSize));
    }
    public void setApacheBufferSize(String apacheBufferSize) {
        this.props.setApacheBufferSize(Integer.parseInt(apacheBufferSize));
    }
    public void setSocketTimeout(String socketTimeout) {
        this.props.setSocketTimeout(Integer.parseInt(socketTimeout));
    }
    public void setConnectionTimeout(String connectionTimeout) {
        this.props.setConnectionTimeout(Integer.parseInt(connectionTimeout));
    }
    public void setDataTransferTimeout(String dataTransferTimeout) {
        this.props.setDataTransferTimeout(Integer.parseInt(dataTransferTimeout));
    }
    public void setKeepAliveTimeout(String keepAliveTimeout) {
        this.props.setKeepAliveTimeout(Integer.parseInt(keepAliveTimeout));
    }
    public void setUser(String user) {
        this.props.setUser(user);
    }
    public void setTimeToLiveMillis(String timeToLiveMillis) {
        this.props.setTimeToLiveMillis(Integer.parseInt(timeToLiveMillis));
    }
    public void setDefaultMaxPerRoute(String defaultMaxPerRoute) {
        this.props.setDefaultMaxPerRoute(Integer.parseInt(defaultMaxPerRoute));
    }
    public void setMaxTotal(String maxTotal) {
        this.props.setMaxTotal(Integer.parseInt(maxTotal));
    }
    public void setMaxCompressBufferSize(String maxCompressBufferSize) {
        this.props.setMaxCompressBufferSize(Integer.parseInt(maxCompressBufferSize));
    }
    public void setSsl(String isSsl) {
        this.props.setSsl(Boolean.parseBoolean(isSsl));
    }
    public void setSslRootCertificate(String sslRootCertificate) {
        this.props.setSslRootCertificate(sslRootCertificate);
    }
    public void setSslMode(String sslMode) {
        this.props.setSslMode(sslMode);
    }
    public void setUseServerTimeZone(String isUseServerTimeZone) {
        this.props.setUseServerTimeZone(Boolean.parseBoolean(isUseServerTimeZone));
    }
    public void setUseTimeZone(String useTimeZone) {
        this.props.setUseTimeZone(useTimeZone);
    }
    public void setUseObjectsInArrays(String isUseObjectsInArrays) {
        this.props.setUseObjectsInArrays(Boolean.parseBoolean(isUseObjectsInArrays));
    }
    public void setUseServerTimeZoneForDates(String isUseServerTimeZoneForDates) {
        this.props.setUseServerTimeZoneForDates(Boolean.parseBoolean(isUseServerTimeZoneForDates));
    }
    public void setMaxParallelReplicas(String maxParallelReplicas) {
        this.props.setMaxParallelReplicas(Integer.parseInt(maxParallelReplicas));
    }
    public void setTotalsMode(String totalsMode) {
        this.props.setTotalsMode(totalsMode); 
    }
    public void setQuotaKey(String quotaKey) {
        this.props.setQuotaKey(quotaKey);
    }
    public void setPriority(String priority) {
        this.props.setPriority(Integer.parseInt(priority));
    }
    public void setDatabase(String database) {
        this.props.setDatabase(database);
    }
    public void setExtremes(String isExtremes) {
        this.props.setExtremes(Boolean.parseBoolean(isExtremes));
    }
    public void setMaxExecutionTime(String maxExecutionTime) {
        this.props.setMaxExecutionTime(Integer.parseInt(maxExecutionTime));
    }
    public void setMaxRowsToGroupBy(String maxRowsToGroupBy) {
        this.props.setMaxRowsToGroupBy(Integer.parseInt(maxRowsToGroupBy));
    }
    public void setPassword(String password) {
        this.props.setPassword(password);
    }
    /* Not applicable for balanced ClickHouse DataSource
    public void setHost(String host) {
        this.host = host;
    }
    public void setPort(int port) {
        this.port = port;
    }
    */
    public void setDistributedAggregationMemoryEfficient(String isDistributedAggregationMemoryEfficient) {
        this.props.setDistributedAggregationMemoryEfficient(Boolean.parseBoolean(isDistributedAggregationMemoryEfficient));
    }
    public void setMaxBytesBeforeExternalGroupBy(String maxBytesBeforeExternalGroupBy) {
        this.props.setMaxBytesBeforeExternalGroupBy(Long.parseLong(maxBytesBeforeExternalGroupBy));
    }
    public void setMaxBytesBeforeExternalSort(String maxBytesBeforeExternalSort) {
        this.props.setMaxBytesBeforeExternalSort(Long.parseLong(maxBytesBeforeExternalSort));
    }
    public void setMaxMemoryUsage(String maxMemoryUsage) {
        this.props.setMaxMemoryUsage(Long.parseLong(maxMemoryUsage));
    }
    public void setPreferredBlockSizeBytes(String preferredBlockSizeBytes) {
        this.props.setPreferredBlockSizeBytes(Long.parseLong(preferredBlockSizeBytes));
    }
    public void setMaxQuerySize(String maxQuerySize) {
        this.props.setMaxQuerySize(Long.parseLong(maxQuerySize));
    }

	// DataSource methods
	
	protected DataSource getDelegate() {
		BalancedClickhouseDataSource localInstance = this.delegate;
		if (localInstance == null) {
			synchronized (ClickhouseJdbcDataSourceWrapper.class) {
				localInstance = this.delegate;
				if (localInstance == null) {
					this.delegate = localInstance = new BalancedClickhouseDataSource(this.url, this.props);
				}
			}
		}
		return localInstance;
	}
	
	@Override
	public PrintWriter getLogWriter() throws SQLException {
		return this.logWriter;
	}

	@Override
	public void setLogWriter(PrintWriter out) throws SQLException {
		this.logWriter = out;
	}

	@Override
	public void setLoginTimeout(int seconds) throws SQLException {
		this.loginTimeout = seconds;
	}

	@Override
	public int getLoginTimeout() throws SQLException {
		return this.loginTimeout;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return this.parentLogger;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return getDelegate().unwrap(iface);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return getDelegate().isWrapperFor(iface);
	}

	@Override
	public Connection getConnection() throws SQLException {
		return getDelegate().getConnection();
	}

	@Override
	public Connection getConnection(String username, String password) throws SQLException {
		return getDelegate().getConnection();
	}

}
