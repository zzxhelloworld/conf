package util;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import conf.FD;
import conf.Utils;

/**
 * Given a schema and FD set,
 * compute all minimal keys
 * @author Zhuoxing Zhang
 *
 */
public class StatMinKeysOnSchemaAndFDs {
	List<FD> fds;
	List<String> schema;
	
	public StatMinKeysOnSchemaAndFDs() throws SQLException, IOException {
		List<Object> res= Utils.import_schema_and_FDs();
		this.schema = (List<String>) res.get(0);
		this.fds = (List<FD>) res.get(1);
	}
	public List<List<String>> computeAllMinKeys(){
		List<String> fristMinKey = Utils.getRefinedMinKey(fds, schema, schema);
		List<List<String>> allMinKeys = Utils.getMinimalKeys(fds, schema, fristMinKey);
		return allMinKeys;
	}
	public static void main(String[] args) throws SQLException, IOException {
		StatMinKeysOnSchemaAndFDs obj = new StatMinKeysOnSchemaAndFDs();
		List<List<String>> minKeys = obj.computeAllMinKeys();
		for(int i = 0;i < minKeys.size();i ++) {
			System.out.println(i+" | "+minKeys.get(i).toString());
		}
	}

}
