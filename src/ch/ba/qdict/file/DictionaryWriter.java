package ch.ba.qdict.file;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DictionaryWriter {

	public static String writeDictToFile(Map<String, Integer> dictMap, String dictFileName, String subfolder, String outPath) {

		System.out.println("Writing Dictionary to File...");
		long dictWriteStartTime = System.nanoTime();
		
		String filePath = outPath + subfolder + "/" + dictFileName;
		File dictFile = new File(filePath);

		BufferedWriter dictWriter = null;
		try {
			if (dictFile.exists()) {
				System.out.println("\tDictionary file with name: " + dictFileName
						+ " already exists. Adding timestamp to new filename.");
				dictFile = new File(filePath + "_" + System.currentTimeMillis());
			}
			dictFile.createNewFile();

			dictWriter = new BufferedWriter(new FileWriter(dictFile));

			for (Map.Entry<String, Integer> entry : dictMap.entrySet()) {
				dictWriter.write(entry.getKey());
				dictWriter.write(" -> ");
				dictWriter.write(entry.getValue().toString());
				dictWriter.newLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		} finally {
			try {
				if (dictWriter != null) {
					dictWriter.close();
				}
			} catch (IOException ex) {
				ex.printStackTrace();
				System.exit(0);
			}
		}

		long dictWriteEndTime = System.nanoTime();
		long dictWriteTime = dictWriteEndTime - dictWriteStartTime;

		System.out.println("\tExecution time: " + TimeUnit.MILLISECONDS.convert(dictWriteTime, TimeUnit.NANOSECONDS)
				+ " ms");

		return dictFile.getAbsolutePath();
	}

}
