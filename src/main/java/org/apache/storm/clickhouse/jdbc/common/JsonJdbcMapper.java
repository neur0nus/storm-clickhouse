package org.apache.storm.clickhouse.jdbc.common;

import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.Util;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.tuple.ITuple;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

@SuppressWarnings("rawtypes")
public class JsonJdbcMapper implements JdbcMapper {

	private static final long serialVersionUID = 2542507881654108237L;
	
	protected final List<Column> schemaColumns = new ArrayList<Column>(); 
	protected final JsonParser parser = new JsonParser();
	protected final SimpleDateFormat FMT_DATE = new SimpleDateFormat("yyyy-MM-dd");
	protected final SimpleDateFormat FMT_DATETIME = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public void initializeColumns(Map<String, String> columnsTypeMap) {
		if (columnsTypeMap==null||columnsTypeMap.isEmpty()) throw new IllegalArgumentException("Columns and types map not specified");
		this.schemaColumns.clear();
		for (Entry<String, String> e:columnsTypeMap.entrySet()) {
			String colName = e.getKey();
			String colType = e.getValue();
			this.schemaColumns.add(new Column(colName, parseSlqType(colType)));
		}
	}
	
	protected static int parseSlqType(String szType) {
        if(szType.equals("String")) return Types.VARCHAR;
        else if(szType.equals("Float32")) return Types.DOUBLE;
        else if(szType.equals("Float64")) return Types.DOUBLE;
        else if(szType.equals("Date")) return Types.DATE;
        else if(szType.equals("DateTime")) return Types.TIMESTAMP;
        else if(szType.equals("Int8")) return Types.SMALLINT;
        else if(szType.equals("Int16")) return Types.SMALLINT;
        else if(szType.equals("Int32")) return Types.INTEGER;
        else if(szType.equals("Int64")) return Types.BIGINT;
        else if(szType.equals("UInt8")) return Types.SMALLINT;
        else if(szType.equals("UInt16")) return Types.SMALLINT;
        else if(szType.equals("UInt32")) return Types.INTEGER;
        else if(szType.equals("UInt64")) return Types.BIGINT;
        else {
            throw new RuntimeException("Unsupported SQL type '"+szType+"'");
        }
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public List<Column> getColumns(ITuple input) {
		try {
			String jsonString = input.getString(0);
			JsonObject jsonObject = parser.parse(jsonString).getAsJsonObject();
	        List<Column> columns = new ArrayList<Column>();
	        for(Column column : schemaColumns) {
	            String columnName = column.getColumnName();
				JsonElement elem = jsonObject.get(columnName);
				if (elem==null) continue;
	            Integer columnSqlType = column.getSqlType();
	            switch (columnSqlType) {
				case Types.VARCHAR:
	                String strValue = elem.getAsString();
	                columns.add(new Column(columnName, strValue, columnSqlType));
					break;
				case Types.FLOAT:
	                Float floatValue = elem.getAsFloat();
	                columns.add(new Column(columnName, floatValue, columnSqlType));
					break;
				case Types.DOUBLE:
	                Double doubleValue = elem.getAsDouble();
	                columns.add(new Column(columnName, doubleValue, columnSqlType));
					break;
				case Types.DATE:
	                Date dateValue = new Date(FMT_DATE.parse(elem.getAsString()).getTime());
	                columns.add(new Column(columnName, dateValue, columnSqlType));
					break;
				case Types.TIMESTAMP:
	                Timestamp timeValue = new Timestamp(elem.getAsLong());
	                columns.add(new Column(columnName, timeValue, columnSqlType));
					break;
				case Types.SMALLINT:
	                Short shortValue = elem.getAsShort();
	                columns.add(new Column(columnName, shortValue, columnSqlType));
					break;
				case Types.INTEGER:
	                Integer intValue = elem.getAsInt();
	                columns.add(new Column(columnName, intValue, columnSqlType));
					break;
				case Types.BIGINT:
	                Long longValue = elem.getAsLong();
	                columns.add(new Column(columnName, longValue, columnSqlType));
					break;
				default:
	                throw new RuntimeException("Unsupported java type in tuple " + Util.getJavaType(columnSqlType));
				}
	        }
	        return columns;
		} catch(Exception err) {
			throw new RuntimeException("JSON parsing error", err);
		}
	}

}
