package conf;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * CONF decomposition with minimizing level l of update inefficiency(Cardinality constraints) and key number
 * @author Zhuoxing Zhang
 *
 */
public class CONF_CC {
	private double ave_comp_min_keys_time_for_noncritical;
	private long exe_time;
	private int max_key_num_on_all_schemata;//maximal number of minimal keys
	private int l_D;//max level on all schemata of D
	private int l_D_total;//sum of level on all schemata of D
	private double ave_l_for_each_schema;
	private double ave_l_for_3NF_schema;
	private int num_of_all_schemata;
	private double ave_key_num_on_all_schemata;
	private int num_of_BCNF_schemata;
	private double ave_key_num_on_BCNF_schemata;
	private int num_of_3NF_schemata;
	private double ave_key_num_on_3NF_schemata;
	private HashMap<Integer,Integer> map_BCNF_keynum_to_num;//in BCNF, the number of N-CONF
	private HashMap<Integer,Integer> map_3NF_keynum_to_num;//in 3NF, the number of N-CONF
	private long time_line_3_to_9;//runtime between line 3 - 9 of pseudo-code
	private long time_line_11_to_12;//sum of time on line 11 - 12
	private long time_line_15_to_19;
	private long time_line_20_to_24;//runtime between line 20 - 24 of pseudo-code
	private long time_line_25;//runtime between line 25 of pseudo-code
	private long time_line_26_to_28;//runtime between line 26 - 28 of pseudo-code
//	private long time_com_level_and_key_num_if_null;//compute remaining level and key number of schema of D if null
	
	
	public CONF_CC() {
		this.ave_comp_min_keys_time_for_noncritical = 0d;
		this.exe_time = 0l;
		this.max_key_num_on_all_schemata = 0;
		this.l_D = 0;
		this.l_D_total = 0;
		this.ave_l_for_each_schema = 0d;
		this.ave_l_for_3NF_schema = 0d;
		this.num_of_all_schemata = 0;
		this.ave_key_num_on_all_schemata = 0d;
		this.num_of_BCNF_schemata = 0;
		this.ave_key_num_on_BCNF_schemata = 0d;
		this.num_of_3NF_schemata = 0;
		this.ave_key_num_on_3NF_schemata = 0d;
		this.map_BCNF_keynum_to_num = new HashMap<Integer,Integer>();
		this.map_3NF_keynum_to_num = new HashMap<Integer,Integer>();
		this.time_line_3_to_9 = 0l;
		this.time_line_11_to_12 = 0l;
		this.time_line_15_to_19 = 0l;
		this.time_line_20_to_24 = 0l;
		this.time_line_25 = 0l;
		this.time_line_26_to_28 = 0l;
//		this.time_com_level_and_key_num_if_null = 0l;
	}
	
	
	public void output_results() throws IOException {
		File f = new File(Constant.output_add);
		FileWriter fw = new FileWriter(f,true);
		BufferedWriter bw = new BufferedWriter(fw);
		
		String dataset_name = Constant.dataset_name;
		
		Iterator<Integer> iter_bcnf = this.map_BCNF_keynum_to_num.keySet().iterator();
		String bcnf_dis = "[";//key number distributions in BCNF
		List<String> bcnf_dis_list = new ArrayList<String>();
		while(iter_bcnf.hasNext()) {
			int key = iter_bcnf.next();
			int value = this.map_BCNF_keynum_to_num.get(key);
			bcnf_dis_list.add(key+" : "+value);
		}
		Collections.sort(bcnf_dis_list,new Comparator<String>() {//decreasing order of n_KEY

			@Override
			public int compare(String o1, String o2) {
				int key1 = Integer.parseInt(o1.split(" : ")[0]);
				int key2 = Integer.parseInt(o2.split(" : ")[0]);
				if(key1 < key2)
					return 1;
				else if(key1 > key2)
					return -1;
				else
					return 0;
			}
			
		});
		for(int i = 0;i < bcnf_dis_list.size();i ++) {
			String s = bcnf_dis_list.get(i);
			if(i != bcnf_dis_list.size() - 1)
				bcnf_dis += "{ " + s + " } ";
			else
				bcnf_dis += "{ " + s + " }";
		}
		bcnf_dis += "]";
		
		Iterator<Integer> iter_3nf = this.map_3NF_keynum_to_num.keySet().iterator();
		String thirdnf_dis = "[";//key number distributions in 3NF
		List<String> thirdnf_dis_list = new ArrayList<String>();
		while(iter_3nf.hasNext()) {
			int key = iter_3nf.next();
			int value = this.map_3NF_keynum_to_num.get(key);
			thirdnf_dis_list.add(key+" : "+value);
		}
		Collections.sort(thirdnf_dis_list,new Comparator<String>() {//decreasing order of n_KEY

			@Override
			public int compare(String o1, String o2) {
				int key1 = Integer.parseInt(o1.split(" : ")[0]);
				int key2 = Integer.parseInt(o2.split(" : ")[0]);
				if(key1 < key2)
					return 1;
				else if(key1 > key2)
					return -1;
				else
					return 0;
			}
			
		});
		for(int i = 0;i < thirdnf_dis_list.size();i ++) {
			String s = thirdnf_dis_list.get(i);
			if(i != thirdnf_dis_list.size() - 1)
				thirdnf_dis += "{ " + s + " } ";
			else
				thirdnf_dis += "{ " + s + " }";
		}
		thirdnf_dis += "]";
		
		
		//a line of results's column names:
		//alg,dataset_name,runtime,average runtime of computing minimal keys for non-critical
		//runtime line 3 - 9,runtime line 11 - 12,runtime line 15 - 19, runtime 20 - 24, runtime line 25, runtime line 26 - 28,
		//l_D,l_D_total,ave_l_for_each_schema,ave_l_for_3NF_schema,max key number on all schemata
		//number of all schemata,average key number on all schemata,
		//number of BCNF schemata,average key number on BCNF schemata,distributions of key number in BCNF
		//number of 3NF schemata,average key number on 3NF schemata,distributions of key number in 3NF
		String result = "CONF_CC,"+dataset_name+","+this.exe_time+","+String.format("%.2f",this.ave_comp_min_keys_time_for_noncritical)+","
		+this.time_line_3_to_9+","+this.time_line_11_to_12+","+this.time_line_15_to_19+","+this.time_line_20_to_24+","+this.time_line_25+","+this.time_line_26_to_28+","
		+this.l_D+","+this.l_D_total+","+String.format("%.2f",this.ave_l_for_each_schema)+","+String.format("%.2f",this.ave_l_for_3NF_schema)+","+this.max_key_num_on_all_schemata+","
		+this.num_of_all_schemata+","+String.format("%.2f",this.ave_key_num_on_all_schemata)+","
		+this.num_of_BCNF_schemata+","+String.format("%.2f",this.ave_key_num_on_BCNF_schemata)+","+bcnf_dis
		+","+this.num_of_3NF_schemata+","+String.format("%.2f",this.ave_key_num_on_3NF_schemata)+","+thirdnf_dis;
		
		bw.write(result+"\n");
		
		bw.close();
		fw.close();
	}
	
	/**
	 * 
	 * @param R schema
	 * @param Sigma functional dependency set 
	 * @param Sigma_a an atomic cover of Sigma
	 * @param r data set
	 * @return
	 */
	public  ArrayList<Schema> exe_decomp(List<String> R,List<FD> Sigma) {
		System.out.println("dataset : "+Constant.file_add);
		System.out.println("FD : "+Constant.fd_add);
		
		List<FD> Sigma_a = new ArrayList<FD>();
		Sigma_a.addAll(Sigma);
		
		long start_time_line_3_to_9 = new Date().getTime();
		
		List<FD> Sigma_c = new ArrayList<FD>();//save critical FD
		
		for(FD X_A : Sigma_a) {
//			List<FD> Sigma_X_A = new ArrayList<FD>();//to save FD that is contained by X -> A if X->A is critical
//			List<Integer> l_X_A_list = new ArrayList<Integer>();
			
			ArrayList<String> XA = new ArrayList<String>();
			for(String a : R) {
				if(X_A.getLeftHand().contains(a)) {
					XA.add(a);
					continue;
				}
				if(X_A.getRightHand().contains(a))
					XA.add(a);
			}
			
//			List<List<String>> r_on_XA = Utils.getSubRelation(R, XA, r);
			
			for(FD Y_B : Sigma_a) {//get Y -> B
				
				ArrayList<String> YB = new ArrayList<String>();
				for(String a : R) {
					if(Y_B.getLeftHand().contains(a)) {
						YB.add(a);
						continue;
					}
					if(Y_B.getRightHand().contains(a))
						YB.add(a);
				}
				
				
				if(XA.containsAll(YB) && !Utils.getAttrSetClosure(R, Y_B.getLeftHand(), Sigma).containsAll(XA)) {//X -> A is critical
					//compute min l_Y 
					//level on XA sub-relation
//					List<String> Y = new ArrayList<String>();
//					for(String a : R) {
//						if(Y_B.getLeftHand().contains(a))
//							Y.add(a);
//					}
//					int l_Y = Utils.getLevelOfAttrSet(XA, Y, r_on_XA) ;
					
//					l_X_A_list.add(l_Y);
					if(!Sigma_c.contains(X_A))
						Sigma_c.add(X_A);
				}
			}
//			int l_X_A = 0;//compute level under X->A
//			if(l_X_A_list.isEmpty()) {//X -> A is not critical
//				
//			}else {//X -> A is critical
//				for(int l_Y : l_X_A_list) {
//					l_X_A = (l_Y > l_X_A ? l_Y : l_X_A);//get max l_Y to l_X_A
//				}
//				X_A.setLevel(l_X_A);//set max l_Y to X -> A
//			}
		}
		long end_time_line_3_to_9 = new Date().getTime();
		this.time_line_3_to_9 = end_time_line_3_to_9 - start_time_line_3_to_9;
		System.out.println("\n=============================\n");
		System.out.println("runtime between line 3 - 9 : "+this.time_line_3_to_9 + " ms.");
		
		List<FD> Sigma_nc = new ArrayList<FD>();//save non-critical FD
		for(FD fd : Sigma_a) {
			if(!Sigma_c.contains(fd))
				Sigma_nc.add(fd);
		}
		
		List<Long> comp_min_keys_time_list_for_noncritical = new ArrayList<Long>();//time list of computing minimal keys 
		for(FD fd : Sigma_nc) {//compute key number of each non-critical FD on Sigma[FD]
			List<String> sub_R = new ArrayList<String>();
			List<String> left = fd.getLeftHand();
			List<String> right = fd.getRightHand();
			for(String a : R) {
				if(left.contains(a)) {
					sub_R.add(a);
					continue;
				}
				if(right.contains(a))
					sub_R.add(a);
			}
			
			long start = new Date().getTime();
			ArrayList<FD> Sigma_a_fd_projection = Utils.getProjection(Sigma_a, sub_R);//get fd projection
			ArrayList<String> minKey = Utils.getRefinedMinKey(Sigma_a_fd_projection, left, sub_R);
			List<List<String>> minKeys = Utils.getMinimalKeys(Sigma_a_fd_projection, sub_R, minKey);
			long end = new Date().getTime();
			long comp_min_keys_time = end - start;//get time of computing minimal keys for a given schema
			
			comp_min_keys_time_list_for_noncritical.add(comp_min_keys_time);
			
			int n_key = minKeys.size();
			fd.setN_key(n_key);//set fd's key number
		}
		
		
		//compute average time of computing each minimal keys
		long sum_time = 0l;
		for(long t : comp_min_keys_time_list_for_noncritical) {
			sum_time += t;
		}
		System.out.println("\n=============================\n");
		this.ave_comp_min_keys_time_for_noncritical = sum_time/(double)comp_min_keys_time_list_for_noncritical.size();
		System.out.println("\n\naverage time of computing each minimal keys : "+String.format("%.2f", ave_comp_min_keys_time_for_noncritical)+" ms.\n");
		
		System.out.println("\n=============================\n");
		this.time_line_11_to_12 = sum_time;
		System.out.println("\n\ntotal time of computing all minimal keys for non-critical schema: "+this.time_line_11_to_12+" ms.\n");
		
		
		ArrayList<Schema> D = new ArrayList<Schema>();
		
		Collections.sort(Sigma_c,new Comparator<FD>() {//decreasing order of level

			@Override
			public int compare(FD o1, FD o2) {
				if(o1.getLevel() < o2.getLevel())
					return 1;
				else if(o1.getLevel() > o2.getLevel())
					return -1;
				else
					return 0;
			}});
		
		Collections.sort(Sigma_nc,new Comparator<FD>() {//decreasing order of key number

			@Override
			public int compare(FD o1, FD o2) {
				if(o1.getN_key() < o2.getN_key())
					return 1;
				else if(o1.getN_key() > o2.getN_key())
					return -1;
				else
					return 0;
			}});
		
		List<FD> Sigma_a_bar = new ArrayList<FD>();
		Sigma_a_bar.addAll(Sigma_a);//alg line 14
		
		long start_time_line_15_to_19 = new Date().getTime();
		for(FD fd : Sigma_c) {//remove higher level redundant FD first
			List<String> left = fd.getLeftHand();
			List<String> right = fd.getRightHand();
			List<FD> Sigma_a_NO_fd = new ArrayList<FD>();
			Sigma_a_NO_fd.addAll(Sigma_a);
			Sigma_a_NO_fd.remove(fd);
			
			List<String> left_closure = Utils.getAttrSetClosure(R, left, Sigma_a_NO_fd);
			if(left_closure.containsAll(right)) {//remove redundant FD
				Sigma_a.remove(fd);
			}else {
				ArrayList<String> subschema = new ArrayList<String>();
				for(String a : R) {
					if(left.contains(a)) {
						subschema.add(a);
						continue;
					}
					if(right.contains(a))
						subschema.add(a);
				}
				ArrayList<FD> projection_fd = Utils.getProjection(Sigma_a_bar, subschema);
				Schema s = new Schema(subschema,projection_fd);//no key number and level
				s.setSchema_level(fd.getLevel());//add level
				
				if(!D.contains(s))
					D.add(s);
			}
		}
		long end_time_line_15_to_19 = new Date().getTime();
		this.time_line_15_to_19 = end_time_line_15_to_19 - start_time_line_15_to_19;
		System.out.println("\n=============================\n");
		System.out.println("runtime between line 15 - 19 : "+this.time_line_15_to_19+" ms.");
		
		long start_time_line_20_to_24 = new Date().getTime();
		for(FD fd : Sigma_nc) {//remove more key number redundant FD first
			List<String> left = fd.getLeftHand();
			List<String> right = fd.getRightHand();
			List<FD> Sigma_a_NO_fd = new ArrayList<FD>();
			Sigma_a_NO_fd.addAll(Sigma_a);
			Sigma_a_NO_fd.remove(fd);
			
			List<String> left_closure = Utils.getAttrSetClosure(R, left, Sigma_a_NO_fd);
			if(left_closure.containsAll(right)) {//remove redundant FD
				Sigma_a.remove(fd);
			}else {
				ArrayList<String> subschema = new ArrayList<String>();
				for(String a : R) {
					if(left.contains(a)) {
						subschema.add(a);
						continue;
					}
					if(right.contains(a))
						subschema.add(a);
				}
				ArrayList<FD> projection_fd = Utils.getProjection(Sigma_a_bar, subschema);
				Schema s = new Schema(subschema,projection_fd,fd.getN_key(),fd.getLevel());//just add schema key number and level
				
				if(!D.contains(s))
					D.add(s);
			}
		}
		long end_time_line_20_to_24 = new Date().getTime();
		this.time_line_20_to_24 = end_time_line_20_to_24 - start_time_line_20_to_24;
		System.out.println("\n=============================\n");
		System.out.println("runtime between line 20 - 24 : "+this.time_line_20_to_24+" ms.");
		
		long start_time_line_25 = new Date().getTime();
		ArrayList<Schema> removal = new ArrayList<Schema>();//need to remove from D
		for(int i = 0;i < D.size();i ++) {
			Schema pair1 = D.get(i);
			List<String> S = pair1.getAttr_set();
			for(int j = 0;j < D.size();j ++) {
				if(i == j)
					continue;
				Schema pair2 = D.get(j);
				List<String> S_prime = pair2.getAttr_set();
				if(S_prime.containsAll(S)) {
					removal.add(pair1);
					break;
				}
			}
		}
		for(Schema p : removal) {
			D.remove(p);
		}
		long end_time_line_25 = new Date().getTime();
		this.time_line_25 = end_time_line_25 - start_time_line_25;
		System.out.println("\n=============================\n");
		System.out.println("runtime on line 25 : "+this.time_line_25+" ms.");
		
		long start_time_line_26_to_28 = new Date().getTime();
		boolean exist = false;//whether existing or not (R',Sigma') in D, such that R' -> R in closure of Sigma (Sigma^+)
		for(Schema p : D) {
			List<String> R_prime = p.getAttr_set();
			ArrayList<String> R_prime_closure = Utils.getAttrSetClosure(R, R_prime, Sigma);
			if(R_prime_closure.containsAll(R)) {
				exist = true;
				break;
			}
		}
		if(!exist) {
			ArrayList<String> K1 = Utils.getRefinedMinKey(Sigma, R, R);
			
			ArrayList<String> K = new ArrayList<String>();
			for(String a : R) {
				if(K1.contains(a))
					K.add(a);
			}
			
			ArrayList<FD> K_projection = Utils.getProjection(Sigma_a_bar, K);
			ArrayList<String> minKey2 = Utils.getRefinedMinKey(K_projection, K, K);
			int n_K = Utils.getMinimalKeys(K_projection, K, minKey2).size();
			D.add(new Schema(K,K_projection,n_K));//D = D U {(K,Sigma_a[K])}
		}
		long end_time_line_26_to_28 = new Date().getTime();
		this.time_line_26_to_28 = end_time_line_26_to_28 - start_time_line_26_to_28;
		System.out.println("\n=============================\n");
		System.out.println("runtime between line 26 - 28 : "+this.time_line_26_to_28+" ms.");
		

		return D;
		
	}
	
	public void decomp_and_output(List<String> R,List<FD> Sigma_a) throws IOException, SQLException {
	    //execute decomposition
	    long start = new Date().getTime();
		ArrayList<Schema> D = exe_decomp(R, Sigma_a);
		long end = new Date().getTime();
		this.exe_time = end - start;
		
		//compute l_D and max key number of D, if level and key number of a schema are null, then compute them
		for(Schema schema : D) {
			schema.calKeyNumAndLevelIfNull(R);
			int schema_level = schema.getSchema_level();
			if(schema_level == 0) {
				System.out.println("debug : level = 0, schema info "+schema.toString());
			}
			int key_num = schema.getN_key();
			this.l_D = schema_level > this.l_D ? schema_level : this.l_D;
			this.l_D_total += schema_level;
			this.max_key_num_on_all_schemata = key_num > this.max_key_num_on_all_schemata ? key_num : this.max_key_num_on_all_schemata;
			
		}
		this.ave_l_for_each_schema = this.l_D_total/((double)D.size());
		
		
		//output statistics
		System.out.println("decompisitions finished, statistics as follows:\n");
		System.out.println("exe time : "+exe_time+" ms.");
		System.out.println("\n=============================\n");
		
		System.out.println("\nmaximal candidate key number for schemata : "+this.max_key_num_on_all_schemata+"\n");
		System.out.println("\nmaximal level for schemata : "+this.l_D+"\n");
		
		ArrayList<Schema> BCNF_schemata = new ArrayList<Schema>();//BCNF databases of all decomposition results
		ArrayList<Schema> thirdNF_schemata = new ArrayList<Schema>();//3NF databases of all decomposition results, not BCNF
		
		System.out.println("\n=============================\n");
		System.out.println("all decomposition results :\n");
		int sum_of_key_num_on_all_decomp = 0;
		for(Schema p : D) {
			List<String> schema = p.getAttr_set();
			List<FD> fds = p.getFd_set();
			System.out.println("schema info : "+p.toString());
			System.out.println("schemata : "+schema +"\nFD set : ");
			for(FD f : fds) {
				System.out.println(f.toString());
			}
			
			boolean isBCNF = Utils.isBCNF(schema,fds);
			if(isBCNF)
				BCNF_schemata.add(p);
			else
				thirdNF_schemata.add(p);
			
			sum_of_key_num_on_all_decomp += p.getN_key();
			System.out.println("number of minimal keys : "+p.getN_key());
			System.out.println("########################\n");
		}
		
		this.ave_key_num_on_all_schemata = sum_of_key_num_on_all_decomp/(double)D.size();
		System.out.println("\n=============================\n");
		System.out.println("average key num on all schematas : "+this.ave_key_num_on_all_schemata);
		
		System.out.println("\n=============================\n");
		this.num_of_all_schemata = D.size();
		System.out.println("final decomposition number of schemata : "+this.num_of_all_schemata);
		
		System.out.println("\n=============================\n");
		this.num_of_BCNF_schemata = BCNF_schemata.size();
		System.out.println("final decomposition number of schemata in BCNF : "+this.num_of_BCNF_schemata);
		
		System.out.println("\n=============================\n");
		System.out.println("BCNF schemata as follows :\n");
		int sum_of_key_num_on_BCNF_decomp = 0;
		for(Schema p : BCNF_schemata) {
			List<String> schema = p.getAttr_set();
			List<FD> fds = p.getFd_set();
			System.out.println("BCNF schema info : "+p.toString());
			System.out.println("BCNF schemata : "+schema +"\nFD set : ");
			for(FD f : fds) {
				System.out.println(f.toString());
			}
			sum_of_key_num_on_BCNF_decomp += p.getN_key();
			
			if(this.map_BCNF_keynum_to_num.containsKey(p.getN_key())) {//record distribution of different key number in BCNF
				int num_keynum = this.map_BCNF_keynum_to_num.get(p.getN_key());
				this.map_BCNF_keynum_to_num.put(p.getN_key(), ++ num_keynum);
			}else {
				this.map_BCNF_keynum_to_num.put(p.getN_key(), 1);
			}
			
			System.out.println("number of minimal keys : "+p.getN_key());
			System.out.println("########################\n");
		}
		
		if(BCNF_schemata.size() != 0)
			this.ave_key_num_on_BCNF_schemata = sum_of_key_num_on_BCNF_decomp/(double)BCNF_schemata.size();
		System.out.println("\n=============================\n");
		System.out.println("average key num on BCNF schematas : "+this.ave_key_num_on_BCNF_schemata);
		
		System.out.println("\n=============================\n");
		this.num_of_3NF_schemata = thirdNF_schemata.size();
		System.out.println("final decomposition number of schemata in 3NF, not in BCNF : "+this.num_of_3NF_schemata);
		
		System.out.println("\n=============================\n");
		System.out.println("3NF (not BCNF) schemata as follows :\n");
		int sum_of_key_num_on_3NF_decomp = 0;
		int sum_3NF_level = 0;
		for(Schema p : thirdNF_schemata) {
			sum_3NF_level += p.getSchema_level();
			List<String> schema = p.getAttr_set();
			List<FD> fds = p.getFd_set();
			System.out.println("3NF schema info : "+p.toString());
			System.out.println("3NF schemata : "+schema +"\nFD set : ");
			for(FD f : fds) {
				System.out.println(f.toString());
			}
			sum_of_key_num_on_3NF_decomp += p.getN_key();
			
			if(this.map_3NF_keynum_to_num.containsKey(p.getN_key())) {//record distribution of different key number in 3NF
				int num_keynum = this.map_3NF_keynum_to_num.get(p.getN_key());
				this.map_3NF_keynum_to_num.put(p.getN_key(), ++ num_keynum);
			}else {
				this.map_3NF_keynum_to_num.put(p.getN_key(), 1);
			}
			
			System.out.println("number of minimal keys : "+p.getN_key());
			System.out.println("########################\n");
		}
		
		if(thirdNF_schemata.size() != 0) {
			this.ave_key_num_on_3NF_schemata = sum_of_key_num_on_3NF_decomp/(double)thirdNF_schemata.size();
			this.ave_l_for_3NF_schema = sum_3NF_level/(double)thirdNF_schemata.size();
		}
		System.out.println("\n=============================\n");
		System.out.println("average key num on 3NF schematas : "+this.ave_key_num_on_3NF_schemata);
		
		//output distributions of key number in BCNF
		System.out.println("\n=============================\n");
		System.out.println("distributions of key number in BCNF : ");
		Iterator<Integer> iter1 = this.map_BCNF_keynum_to_num.keySet().iterator();
		while(iter1.hasNext()) {
			int key = iter1.next();
			int value = this.map_BCNF_keynum_to_num.get(key);
			System.out.println("key number of subschema : "+key+" , number : "+value);
		}
		
		//output distributions of key number in 3NF
		System.out.println("\n=============================\n");
		System.out.println("distributions of key number in 3NF : ");
		Iterator<Integer> iter2 = this.map_3NF_keynum_to_num.keySet().iterator();
		while(iter2.hasNext()) {
			int key = iter2.next();
			int value = this.map_3NF_keynum_to_num.get(key);
			System.out.println("key number of subschema : "+key+" , number : "+value);
		}
		
		//output results into file
		this.output_results();
		
	}
	

	public static void main(String[] args) throws IOException, SQLException {
//		CONF_CC cc = new CONF_CC();
//		cc.decomp_and_output();
	}

}
