package util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Random;

import conf.Constant;
/**
 * we study key numbers influence on tpc benchmark
 * @author Zhuoxing Zhang
 *
 */
public class KeyNumExpOnLineitem {
	
	/**
	 * 获得链表数组的中位数
	 * @param list
	 * @return
	 */
	public static double getMedian(List<Double> list) {
		Collections.sort(list,new Comparator<Double>(){

			@Override
			public int compare(Double o1, Double o2) {
				if(o1 > o2)
					return 1;
				else if(o1 < o2)
					return -1;
				else
					return 0;
			}
		});
		int middleIndex = -1;
		if(list.size() % 2 == 0)
			middleIndex = list.size()/2 -1;
		else
			middleIndex = list.size()/2;

		return list.get(middleIndex);
	}
	
	
	public static double getAve(List<Double> list) {
		DecimalFormat df = new DecimalFormat("0.00");
		double sum = 0;
		for(double data : list) {
			sum += data;
		}
		return Double.parseDouble(df.format(sum/list.size()));
	}
	
	public static Connection connectDB() {
		String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";  
	    String DB_URL = "jdbc:mysql://localhost:3306/freeman?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
	    		+ "&useServerPrepStmts=true&cachePrepStmts=true&rewriteBatchedStatements=true";
	    String USER = "root";
	    String PASS = "zzxzzx";
	    Connection conn = null;
        try{
            Class.forName(JDBC_DRIVER);
        
            conn = DriverManager.getConnection(DB_URL,USER,PASS);
        }catch(SQLException se){
            se.printStackTrace();
        }catch(Exception e){
            e.printStackTrace();
        }
        return conn;

	}
	
	/**
	 * 
	 * @param tableName
	 * @param dataset
	 * @return cost time for inserting records(ms)
	 * @throws SQLException
	 */
	public static double insert_data(String tableName,List<List<String>> dataset) throws SQLException {
		if(dataset == null)
			return -1;
		if(dataset.isEmpty())
			return -1;
		Connection conn = connectDB();
		conn.setAutoCommit(false);//manual commit
		String insertSql = "INSERT INTO `"+tableName + "` VALUES ( null, ";//for id we set auto-increment
		for(int i = 0;i < dataset.get(0).size();i ++) {
			if(i != (dataset.get(0).size() - 1))
				insertSql += " ? ,";
			else
				insertSql += " ? )";
		}
		PreparedStatement prepStmt1 = conn.prepareStatement(insertSql);
		System.out.println("\n==================");
		System.out.println("insert "+dataset.size()+" records into table : "+tableName);
		System.out.println(insertSql);
		
		long start = System.currentTimeMillis();
		int count = 0;
		for(List<String> data : dataset) {
			count ++;
			for(int i = 1;i <= data.size();i ++) {
				prepStmt1.setString(i, data.get(i-1));
			}
			prepStmt1.addBatch();//batch process
			if(count % 10000 == 0) {
				prepStmt1.executeBatch();
				conn.commit();
				prepStmt1.clearBatch();
			}
		}
		prepStmt1.executeBatch();
		conn.commit();//commit
		long end = System.currentTimeMillis();
		System.out.println("==================\n");
		prepStmt1.close();
		conn.close();
		return (double)(end - start);
	}
	
	/**
	 * delete all data from table with id greater than an integer
	 * @param tableName
	 * @param dataset
	 * @param table_row_num the original size of data set table
	 * @throws SQLException 
	 */
	public static void delete_data(String tableName,List<List<String>> dataset,int table_row_num) throws SQLException {
		if(dataset == null)
			return;
		if(dataset.isEmpty())
			return;
		Connection conn = connectDB();
		System.out.println("\n==================");
		System.out.println("delete "+dataset.size()+" records from table : "+tableName);
		String delSql = "DELETE FROM `"+tableName + "` WHERE `id` > "+table_row_num;//delete records

		System.out.println(delSql);
		Statement stmt = conn.createStatement();
		stmt.executeUpdate(delSql);
		
		System.out.println("==================\n");
		stmt.close();
		conn.close();
	}
	
	/**
	 * 执行查询语句 验证key
	 * SELECT R.A1,...,R.An,count(*) FROM R GROUP BY R.A1,...,R.An HAVING COUNT(*) > 1
	 * @param key
	 * @param tableName
	 * @return 查询时间
	 * @throws SQLException
	 */
	public static double select_one(List<String> key,String tableName) throws SQLException {
		Connection conn = connectDB();
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		String sql = "SELECT ";
		for(String attr : key) {
			sql += "`"+tableName+"`.`"+attr+"`, ";
		}
		sql += "COUNT(*) FROM `"+tableName+"` GROUP BY ";
		for(int i = 0;i < key.size();i ++) {
			String attr = key.get(i);
			if(i != (key.size() - 1))
			    sql += "`"+tableName+"`.`"+attr+"`, ";
			else
				sql += "`"+tableName+"`.`"+attr+"` ";
		}
		sql += "HAVING COUNT(*) > 1";
		
		System.out.println("\n==================");
		System.out.println(sql);
		
		long start = new Date().getTime();
		ResultSet rs = stmt.executeQuery(sql);
		rs.last();
		System.out.println("search result size: "+rs.getRow());
		long end = new Date().getTime();
		
		rs.close();
		stmt.close();
		conn.close();
		System.out.println("execution time(ms): "+(end - start));
		System.out.println("==================\n");
		return (double)(end - start);
	}
	
	/**
	 * 执行查询语句
	 * SELECT R.A1,...,R.An FROM R, R AS R1 WHERE R.A1 = R1.A1 AND ... AND R.An = R1.An AND R.id <> R1.id
	 * @param key
	 * @param tableName
	 * @return 查询时间
	 * @throws SQLException
	 */
	public static double select_two(List<String> key,String tableName) throws SQLException {
		Connection conn = connectDB();
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		String sql = "SELECT ";
		for(int i = 0;i < key.size();i ++) {
			String attr = key.get(i);
			if(i != (key.size() - 1))
			    sql += "`"+tableName+"`.`"+attr+"`, ";
			else
				sql += "`"+tableName+"`.`"+attr+"` ";
		}
		sql += "FROM `"+tableName+"` , `"+tableName+"` AS `"+tableName+"_1` WHERE ";
		for(int i = 0;i < key.size();i ++) {
			String attr = key.get(i);
			if(i != (key.size() - 1))
			    sql += "`"+tableName+"`.`"+attr+"` = `"+tableName+"_1`.`"+attr+"` AND ";
			else
				sql += "`"+tableName+"`.`"+attr+"` = `"+tableName+"_1`.`"+attr+"`";
		}
		sql += " AND `"+tableName+"`.`id` <> `"+tableName+"_1`.`id`";
		System.out.println("\n==================");
		System.out.println(sql);
		
		long start = new Date().getTime();
		ResultSet rs = stmt.executeQuery(sql);
		rs.last();
		System.out.println("search result size: "+rs.getRow());
		long end = new Date().getTime();
		
		rs.close();
		stmt.close();
		conn.close();
		System.out.println("execution time(ms): "+(end - start));
		System.out.println("==================\n");
		return (double)(end - start);
	}
	
	/**
	 * specify a set of attributes (we call key) as an unique constraint on a table
	 * @param key
	 * @param tableName
	 * @throws SQLException
	 */
	public static void add_unique_constraint(List<String> key,String tableName,String unique_id) throws SQLException {
		Connection conn = connectDB();
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		
		String unique_constraint ="ALTER TABLE `"+tableName+"` ADD UNIQUE `"+unique_id+"` ( ";//create an unique constraint
		for(int i =0;i < key.size();i ++) {
			if(i != (key.size() -1))
				unique_constraint += "`"+key.get(i) + "`,";
			else
				unique_constraint += "`"+key.get(i) + "` )";
		}
		
		System.out.println("\n==================");
		System.out.println("create an unique constarint : ");
		System.out.println(unique_constraint);
		
		stmt.executeUpdate(unique_constraint);
		
		System.out.println("==================\n");
		
		stmt.close();
		conn.close();
	}
	
	/**
	 * remove an unique constraint on a table
	 * @param tableName
	 * @param unique_id
	 * @throws SQLException
	 */
	public static void del_unique_constraint(String tableName,String unique_id) throws SQLException {
		Connection conn = connectDB();
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		
		String del_unique_constraint = "DROP INDEX `"+unique_id+"` ON `"+tableName+"`";//delete an unique constraint
		
		System.out.println("\n==================");
		System.out.println("delete an unique constarint : ");
		System.out.println(del_unique_constraint);
		
		stmt.executeUpdate(del_unique_constraint);
		
		System.out.println("==================\n");
		
		stmt.close();
		conn.close();
	}
	
	/**
	 * 
	 * @param col_num a data's column number
	 * @param row_num data number of the data set
	 * @return a data set with fixed col_num and row_num
	 */
	public static List<List<String>> gen_inserted_dataset(int row_num,int col_num){
		List<List<String>> dataset = new ArrayList<List<String>>();
		for(int i = 0;i < row_num;i ++) {
			List<String> data = new ArrayList<String>();
			for(int j = 0;j < col_num;j ++) {
				data.add(i+"_"+j);
			}
			dataset.add(data);
		}
		return dataset;
	}
	
	
	public static List<List<String>> import_keys_from_local(String add) throws IOException{
		List<List<String>> keys = new ArrayList<List<String>>();
		FileReader fr = new FileReader(add);
		BufferedReader br = new BufferedReader(fr);
		String line;
		while((line = br.readLine()) != null) {
			//a line is like "0 | [10, 11, 12, 14, 15]"
			String l = line.split(" \\| ")[1].replace("[", "").replace("]", "").replace(" ", "");
			String[] key_array = l.split(",");
			List<String> key = new ArrayList<String>(Arrays.asList(key_array));
			if(!keys.contains(key))
				keys.add(key);
  		}
		br.close();
		fr.close();
		return keys;
	}
	
	/**
	 * execute single experiments with fixed key number
	 * @param tableName
	 * @param table_col_num
	 * @param table_row_num
	 * @param inserted_row_num
	 * @param keyNum
	 * @param all_keys
	 * @return a result list in form of <q1_median,q1_ave,q2_median,q2_ave,insert_time1,insert_time2,insert_time3>
	 * @throws SQLException
	 */
	public static List<Double> single_query_and_insert_exp_with_fixed_key_num(int round,String tableName,int table_col_num,int table_row_num,List<Integer> inserted_row_num,int keyNum,List<List<String>> all_keys) throws SQLException {
		List<List<String>> selected_keys = new ArrayList<List<String>>();//keys we used for experiments
		Random random = new Random();
		while(selected_keys.size() != keyNum) {
			int i = random.nextInt(all_keys.size());
			if(!selected_keys.contains(all_keys.get(i)))
				selected_keys.add(all_keys.get(i));
		}
		System.out.println("##############");
		System.out.println("key number | "+keyNum);
		
		//add n key numbers in form of unique constraints
		for(int i = 0;i < selected_keys.size();i ++) {
			List<String> key = selected_keys.get(i);
			System.out.println("adding unique constraint \"uc_"+i+"\"...");
			add_unique_constraint(key, tableName, "uc_"+i);	//we set unique id as uc_0,uc_1,....
		}
		System.out.println("finished to add "+keyNum+" unqiue constraints...");
		
		//query experiment
		List<Double> query_one_time = new ArrayList<Double>();
		List<Double> query_two_time = new ArrayList<Double>();
		for(List<String> key : selected_keys) {
			double q1_time = select_one(key,tableName);
			double q2_time = select_two(key,tableName);
			query_one_time.add(q1_time);
			query_two_time.add(q2_time);
		}
		
		//insert experiment
		List<Double> update_time = new ArrayList<Double>();
		for(int row_num : inserted_row_num) {//insert row_num records
			//generate inserted row_num records
			List<List<String>> inserted_dataset = gen_inserted_dataset(row_num,table_col_num);
			//insert data set and return cost time
			double insert_time = insert_data(tableName,inserted_dataset);
			update_time.add(insert_time);
			//delete all inserted records
			delete_data(tableName,inserted_dataset,table_row_num);
		}
		
		//delete all unique constraints
		for(int i = 0;i < selected_keys.size();i ++) {
			System.out.println("deleting unique constraint \"uc_"+i+"\"...");
			del_unique_constraint(tableName, "uc_"+i);	//we set unique id as uc_0,uc_1,....
		}
		System.out.println("finished to delete "+keyNum+" unqiue constraints...");
		
		//stat query time for each query
		double q1m = getMedian(query_one_time);
		double q1a = getAve(query_one_time);
		double q2m = getMedian(query_two_time);
		double q2a = getAve(query_two_time);
		
		List<Double> result_list = new ArrayList<Double>();
		result_list.add(q1m);result_list.add(q1a);result_list.add(q2m);result_list.add(q2a);
		String result = "";
		result += q1m+","+q1a+","+q2m+","+q2a;
		
		for(Double t : update_time) {
			result += ","+t;
			result_list.add(t);
		}
		
		System.out.println("\nround = "+round+" | key num = "+keyNum+" stats : "+result);
		System.out.println("##############");
		return result_list;
	}
	
	/**
	 * execute specific round random key experiments
	 * @param round experiment rounds with fixed key number
	 * @param tableName
	 * @param table_col_num
	 * @param table_row_num
	 * @param inserted_row_num
	 * @param keyNum
	 * @param all_keys
	 * @return a result list in form of <q1_median,q1_ave,q2_median,q2_ave,
	 * insert_time1_median,insert_time1_ave,insert_time2_median,insert_time2_ave,insert_time3_median,insert_time3_ave>
	 * @throws SQLException
	 */
	public static List<Double> query_and_insert_exp_with_fixed_key_num(int round,String tableName,int table_col_num,int table_row_num,List<Integer> inserted_row_num,int keyNum,List<List<String>> all_keys) throws SQLException{
		List<Double> output = new ArrayList<Double>();
		
		List<Double> q1m = new ArrayList<Double>();//query time for each key
		List<Double> q1a = new ArrayList<Double>();
		List<Double> q2m = new ArrayList<Double>();
		List<Double> q2a = new ArrayList<Double>();
		List<Double> u1 = new ArrayList<Double>();//update time for inserting data set
		List<Double> u2 = new ArrayList<Double>();
		List<Double> u3 = new ArrayList<Double>();
		for(int i = 0;i < round;i ++) {
			System.out.println("round : "+i+" | key num : " + keyNum);
			List<Double> res = single_query_and_insert_exp_with_fixed_key_num(i,tableName,table_col_num,table_row_num,inserted_row_num,keyNum,all_keys);
			q1m.add(res.get(0));q1a.add(res.get(1));q2m.add(res.get(2));q2a.add(res.get(3));
			u1.add(res.get(4));u2.add(res.get(5));u3.add(res.get(6));
			System.out.println("******************\n");
		}
		double q1m_a = getMedian(q1m);
		double q1a_a = getAve(q1a);
		double q2m_a = getMedian(q2m);
		double q2a_a = getAve(q2a);
		double u1m = getMedian(u1);
		double u1a = getAve(u1);
		double u2m = getMedian(u2);
		double u2a = getAve(u2);
		double u3m = getMedian(u3);
		double u3a = getAve(u3);
		
		String stat = q1m_a+","+q1a_a+","+q2m_a+","+q2a_a+","+u1m+","+u1a+","+u2m+","+u2a+","+u3m+","+u3a;
		output.add(q1m_a);output.add(q1a_a);output.add(q2m_a);output.add(q2a_a);
		output.add(u1m);output.add(u1a);
		output.add(u2m);output.add(u2a);
		output.add(u3m);output.add(u3a);
		
		System.out.println("\nkey num = "+keyNum+" stats : "+stat);
		System.out.println("==========================");
		return output;
	}
	
	public static List<List<Double>> query_and_insert_exp(List<Integer> key_num_list,int round,String tableName,int table_col_num,int table_row_num,List<Integer> inserted_row_num,List<List<String>> all_keys) throws SQLException{
		List<List<Double>> all_results = new ArrayList<List<Double>>();
		for(int key_num : key_num_list) {
			System.out.println("key num : "+key_num);
			List<Double> res = query_and_insert_exp_with_fixed_key_num(round, tableName, table_col_num, table_row_num, inserted_row_num, key_num, all_keys);
			all_results.add(res);
		}
		return all_results;
	}
	
	//the table should add a new column as id,but not counted in table column number
	public static void main(String[] args) throws IOException, SQLException {
		List<Integer> key_num_list = Arrays.asList(1,5,9,13);
		int round = 10;//for fixed key number, randomly generate 'round' different key sets to experiment
		String tableName = Constant.dataset_name;
		int table_col_num = Constant.col_num;
		int table_row_num = Constant.row_num;
		List<Integer> inserted_row_num = Arrays.asList(60000,120000,180000);
		List<List<String>> all_keys = null;
		boolean import_keys_from_local = true;
		String key_path = "C:\\Users\\wang\\Desktop\\phd论文\\CONF的相关工作\\CONF会议版本\\SIGMOD revision\\lineitem_keys.txt";
		
		if(import_keys_from_local)//import lineitem keys
			all_keys = KeyNumExpOnLineitem.import_keys_from_local(key_path);
		else {
			StatMinKeysOnSchemaAndFDs obj = new StatMinKeysOnSchemaAndFDs();
			all_keys = obj.computeAllMinKeys();
		}
		
		System.out.println("key number of "+tableName+" : "+all_keys.size());
		
		//start experiment
		List<List<Double>> results = KeyNumExpOnLineitem.query_and_insert_exp(key_num_list, round, tableName, table_col_num, table_row_num, inserted_row_num, all_keys);
		System.out.println("\n#######################");
		System.out.println("final stats : \n");
		for(int i = 0;i < results.size();i ++) {
			System.out.println("key num = "+key_num_list.get(i)+" | stat : "+results.get(i)+"\n");
		}
		System.out.println("\n#######################");

	}

}
