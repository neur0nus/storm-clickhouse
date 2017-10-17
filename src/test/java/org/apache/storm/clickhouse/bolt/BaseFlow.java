package org.apache.storm.clickhouse.bolt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class BaseFlow {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void main(String[] args) throws Exception {
		Map configMap = Maps.newHashMap();
		// Hikari properties
		configMap.put("dataSource.url", "jdbc:clickhouse://127.0.0.1:8123/test");
		configMap.put("dataSource.user","default");
		configMap.put("dataSource.password","");
		// ClickHouse properties
		configMap.put("dataSource.compress","true");

		Map<Integer, String> taskToComponent = new HashMap<Integer, String>();
		taskToComponent.put(777, "clickhouse-test-bolt");
		Map<String, List<Integer>> componentToSortedTasks = new HashMap<String, List<Integer>>();
		componentToSortedTasks.put("clickhouse-test-bolt", Arrays.asList(new Integer[]{777}));
		Map<String, Map<String, Fields>> componentToStreamToFields = new HashMap<String, Map<String, Fields>>();
		Map<String,Fields> s2f = new HashMap<String,Fields>();
		//s2f.put("stream-to-clickhouse", new Fields("t_date","stockid","symbol","strike","calc_date_time"));
		s2f.put("stream-to-clickhouse", new Fields("json"));
		componentToStreamToFields.put("clickhouse-test-bolt", s2f);
		TopologyContext tctx = new TopologyContext(null, 
				configMap, 
				taskToComponent, 
				componentToSortedTasks, 
				componentToStreamToFields, 
				"sid", 
				"/tmp", 
				"xxx", 
				777, 
				6627, 
				null, 
				null, 
				null, 
				null, 
				null, 
				null);
		OutputCollector out = new OutputCollector(new IOutputCollector() {
			public void reportError(Throwable error) {
				error.printStackTrace();
			}
			public void resetTimeout(Tuple input) {}
			public void fail(Tuple input) {
				System.err.println("FAIL!");
			}
			public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {}
			public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
				return null;
			}
			public void ack(Tuple input) {
				System.out.println("ACK");
			}
		});
		
		configMap.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, "3");
		
		/*
		List<Column> columnSchema = Lists.newArrayList(
			    new Column("t_date", java.sql.Types.DATE),
			    new Column("stockid", java.sql.Types.INTEGER),
			    new Column("symbol", java.sql.Types.VARCHAR),
			    new Column("strike", java.sql.Types.FLOAT),
			    new Column("calc_date_time", java.sql.Types.TIMESTAMP)
		);
		JdbcMapper jdbcMapper = new SimpleJdbcMapper(columnSchema);
		*/
		ClickhouseInsertBolt bolt = new ClickhouseInsertBolt(configMap/*, jdbcMapper*/)
				.withTableName("options");
		
		bolt.prepare(configMap, tctx, out);
		
		List<Object> values = new ArrayList<Object>();
		/*
		values.add(new Date().getTime());
		values.add(12345);
		values.add("AAPL.");
		values.add(1000.0);
		values.add(System.currentTimeMillis());
		*/
		values.add("{\"t_date\":'2017-10-12',\"stockid\":32453,\"symbol\":\"ABCD.\",\"strike\":1631,\"calc_date_time\":"+System.currentTimeMillis()+"}");
		Tuple tuple = new TupleImpl(tctx, values, 777, "stream-to-clickhouse");
		
		bolt.execute(tuple);
		
		bolt.cleanup();
		
	}
	
}
