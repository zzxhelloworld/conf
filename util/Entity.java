package util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * an entity consists of entity id,schema string, and schema level
 * @author Zhuoxing Zhang
 *
 */
public class Entity {
	private int id;
	private String schemaStr;
	private int schemaLevel;
	
	public Entity(int id, String schemaStr, int schemaLevel) {
		this.id = id;
		this.schemaStr = schemaStr;
		this.schemaLevel = schemaLevel;
	}
	

	public int getId() {
		return id;
	}



	public void setId(int id) {
		this.id = id;
	}



	public String getSchemaStr() {
		return schemaStr;
	}



	public void setSchemaStr(String schemaStr) {
		this.schemaStr = schemaStr;
	}



	public int getSchemaLevel() {
		return schemaLevel;
	}



	public void setSchemaLevel(int schemaLevel) {
		this.schemaLevel = schemaLevel;
	}
	
	/**
	 * according to the directory, access each text file in it to get all entity info.
	 * @param dir
	 * @return list of entity
	 * @throws IOException 
	 */
	public static void import_entity_from_local_dir(List<Entity> entities,String dir,String split) throws IOException{
//		List<Entity> entities = new ArrayList<Entity>();
		File f = new File(dir);
		if(f.isFile()) {
			Entity.read_entity_info_from_local_txt(entities, f.getAbsolutePath(),split);
		}else {
			for(File sub_f : f.listFiles()) {
				Entity.import_entity_from_local_dir(entities,sub_f.getAbsolutePath(),split);
			}
		}
//		return entities;
	}
	
	public static void read_entity_info_from_local_txt(List<Entity> entities,String filePath,String split) throws IOException {
		FileReader fr = new FileReader(filePath);
		BufferedReader br = new BufferedReader(fr);
		String line;
		while((line = br.readLine()) != null) {
			String[] eles = line.split(split);
			if(eles.length != 3) {
				System.out.println("entity ["+line+"] is incorrect!");
				continue;
			}
			if("id".equals(eles[0])) {
				continue;
			}else {
				Entity entity = new Entity(Integer.parseInt(eles[0]),eles[1],Integer.parseInt(eles[2]));
				entities.add(entity);
			}
		}
		br.close();
		fr.close();
	}
	
	public static void write_entity_info_into_local_txt(List<Entity> entities,String fileAdd,String split) throws IOException {
		entities.sort(new Comparator<Entity>() {
			//increasing order by id of entity
			@Override
			public int compare(Entity o1, Entity o2) {
				if(o1.getId() > o2.getId())
					return 1;
				else if(o1.getId() < o2.getId())
					return -1;
				else				
					return 0;
			}
			
		});
		File f = new File(fileAdd);
		if(!f.exists())
			f.createNewFile();
		FileWriter fw = new FileWriter(fileAdd);
		BufferedWriter bw = new BufferedWriter(fw);
		for(Entity e : entities) {
//			System.out.println("debug");
			bw.write(e.getId()+split+e.getSchemaStr()+split+e.getSchemaLevel()+"\n");
		}
		bw.close();
		fw.close();
	}
	
	/**
	 * aggregate all files into a file to get a complete schema level file
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		String root = "C:\\Users\\wang\\Desktop\\phd论文\\CONF的相关工作\\CONF期刊版本\\dataset\\subschema_level\\lineitem2";
		String output = "C:\\Users\\wang\\Desktop\\phd论文\\CONF的相关工作\\CONF期刊版本\\dataset\\subschema_level\\lineitem.txt";
		String split = ";";
		List<Entity> entities = new ArrayList<Entity>();
		Entity.import_entity_from_local_dir(entities,root, split);
		Entity.write_entity_info_into_local_txt(entities, output, split);

	}

}
