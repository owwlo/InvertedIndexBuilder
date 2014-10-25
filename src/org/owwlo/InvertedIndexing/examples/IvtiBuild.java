
package org.owwlo.InvertedIndexing.examples;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.owwlo.InvertedIndexing.InvertedIndexBuilder;
import org.owwlo.InvertedIndexing.InvertedIndexBuilder.IvtMapInteger;

public class IvtiBuild {
    public static Random rand = new Random();

    public static String getRandomString(int length) {
        String alphabet = "abcdefghijklmnopqr";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(alphabet.charAt(rand.nextInt(alphabet.length())));
        }
        return sb.toString();
    }

    public static void cleanUpDirectory(File dir) {
        dir.mkdirs();
        for (File file : dir.listFiles()) {
            file.delete();
        }
    }

    public static void main(String[] aregs) throws IOException {
        long start_t = System.currentTimeMillis();

        final int fakeFileCount = 10228;
        final int fakePassageLength = 2000;
        final int termMaxLength = 5;
        final int filesPerBatch = 1000;

        // InvertedIndexBuilder builder = InvertedIndexBuilder.getBuilder(new
        // File(System
        // .getProperty("java.io.tmpdir")));
        File dir = new File("test");
        cleanUpDirectory(dir);
        InvertedIndexBuilder builder = InvertedIndexBuilder.getBuilder(dir);

        for (int batchNum = 0; batchNum < fakeFileCount / filesPerBatch + 1; batchNum++) {
            int fileIdStart = batchNum * filesPerBatch;
            int fileIdEnd = (batchNum + 1) * filesPerBatch;
            if (fileIdEnd > fakeFileCount) {
                fileIdEnd = fakeFileCount;
            }

            System.out.println("Processing files from " + fileIdStart + " to " + fileIdEnd);

            IvtMapInteger ivtMapBatch = builder.createDistributedIvtiIntegerMap();

            Map<String, List<Integer>> ivtMap = new LinkedHashMap<String, List<Integer>>();

            for (int docId = fileIdStart; docId < fileIdEnd; docId++) {
                Map<String, List<Integer>> ivtMapItem = new HashMap<String, List<Integer>>();

                for (int tokenIdx = 0; tokenIdx < fakePassageLength; tokenIdx++) {
                    String token = getRandomString(rand.nextInt(termMaxLength) + 1);

                    if (!ivtMapItem.containsKey(token)) {
                        ivtMapItem.put(token, new LinkedList<Integer>());
                    }
                    ivtMapItem.get(token).add(tokenIdx);
                }

                for (String token : ivtMapItem.keySet()) {
                    if (!ivtMap.containsKey(token)) {
                        ivtMap.put(token, new LinkedList<Integer>());
                    }
                    List<Integer> recordList = ivtMap.get(token);
                    for (int idx : ivtMapItem.get(token)) {
                        recordList.add(docId);
                        recordList.add(idx);
                    }
                }
            }

            System.out.println("Batch building done. Elapsed: "
                    + (System.currentTimeMillis() - start_t) / 1000.0 + "s");

            ivtMapBatch.putAll(ivtMap);
            ivtMapBatch.close();

            System.out.println("Batch commit done. Elapsed: "
                    + (System.currentTimeMillis() - start_t) / 1000.0 + "s");
        }

        builder.close();

        System.out.println("Construct done. Duration: " + (System.currentTimeMillis() - start_t)
                / 1000.0 + "s");
    }
}
