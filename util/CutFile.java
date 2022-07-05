package util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CutFile {
	
	/**
	 * get specific lines of a file
	 * @param input
	 * @param cut_start
	 * @param cut_end
	 * @param output
	 * @throws IOException
	 */
	public static void cut(String input,int cut_start,int cut_end,String output) throws IOException {
		FileReader fr = new FileReader(input);
		BufferedReader br = new BufferedReader(fr);
		
		List<String> content = new ArrayList<String>();
		int count = 0;
		String line;
		while((line = br.readLine()) != null) {
			count ++;
			if(count >= cut_start && count <= cut_end) {
				content.add(line);
			}
		}
		br.close();
		fr.close();
		
		File file = new File(output);
		if(!file.exists())
			file.createNewFile();
		FileWriter fw = new FileWriter(output);
		BufferedWriter bw = new BufferedWriter(fw);
		for(String l : content) {
			bw.write(l+"\n");
		}
		bw.close();
		fw.close();
	}
	public static void main(String[] args) throws IOException {
		String input = "C:\\Users\\Admin\\Desktop\\SIGMOD revision\\second experiment\\CONF-BCNF-decomp.txt";
		cut(input,5202,9582,input);

	}

}
