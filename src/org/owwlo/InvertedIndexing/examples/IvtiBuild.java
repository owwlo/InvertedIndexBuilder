
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

    public static void printList(List<Integer> l) {
        for (int i : l) {
            System.out.print(i + " ");
        }
        System.out.println();
    }

    public static void main(String[] aregs) throws IOException {
        long start_t = System.currentTimeMillis();

        final int fakeFileCount = 10228;
        final int fakePassageLength = 2000;
        final int termMaxLength = 5;
        final int filesPerBatch = 1000;

        String randPickToken = null;

        // Set directory which the indexes will be writen to.
        File dir = new File("test");

        // Remove all files in this directory.
        cleanUpDirectory(dir);

        // Create an instance of InvertedIndexBuilder.
        InvertedIndexBuilder builder = InvertedIndexBuilder.getBuilder(dir);

        // Divide the whole task into parts.
        // This also allow you to process files with multiple threads.
        for (int batchNum = 0; batchNum < fakeFileCount / filesPerBatch + 1; batchNum++) {
            int fileIdStart = batchNum * filesPerBatch;
            int fileIdEnd = (batchNum + 1) * filesPerBatch;
            if (fileIdEnd > fakeFileCount) {
                fileIdEnd = fakeFileCount;
            }

            System.out.println("Processing files from " + fileIdStart + " to " + fileIdEnd);

            // Create a Inverted Index Map.
            IvtMapInteger ivtMapBatch = builder.createDistributedIvtiIntegerMap();

            // This is used to collect result for each batch.
            Map<String, List<Integer>> ivtMap = new LinkedHashMap<String, List<Integer>>();

            // Simulation of processing each file.
            for (int docId = fileIdStart; docId < fileIdEnd; docId++) {

                // Record Inverted Index for each article.
                Map<String, List<Integer>> ivtMapItem = new HashMap<String, List<Integer>>();

                for (int tokenIdx = 0; tokenIdx < fakePassageLength; tokenIdx++) {
                    String token = getRandomString(rand.nextInt(termMaxLength) + 1);

                    // Randomly chose a token for test purpose.
                    if (docId >= fileIdEnd / 2 && randPickToken == null
                            && token.length() >= termMaxLength * 0.75) {
                        randPickToken = token;
                        System.out.println("Chose token for test: " + randPickToken);
                    }

                    if (!ivtMapItem.containsKey(token)) {
                        ivtMapItem.put(token, new LinkedList<Integer>());
                    }
                    ivtMapItem.get(token).add(tokenIdx);
                }

                // Put collected data into ivtMap.
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

                // Ensure randPickToken will be assigned with a value.
                if (randPickToken == null) {
                    randPickToken = ivtMap.keySet().iterator().next();
                    System.out.println("Chose token for test: " + randPickToken);
                }
            }

            if (ivtMap.containsKey(randPickToken)) {
                List<Integer> testList = ivtMap.get(randPickToken);
                System.out.println("Posting List in batch " + batchNum + " for token '"
                        + randPickToken
                        + "':");
                printList(testList);
            }

            System.out.println("Batch building done. Elapsed: "
                    + (System.currentTimeMillis() - start_t) / 1000.0 + "s");

            // Build Inverted Index for one batch.
            ivtMapBatch.putAll(ivtMap);
            ivtMapBatch.close();

            System.out.println("Batch commit done. Elapsed: "
                    + (System.currentTimeMillis() - start_t) / 1000.0 + "s");
        }

        System.out.println("Start constructing secondary index. Elapsed: "
                + (System.currentTimeMillis() - start_t) / 1000.0 + "s");

        // IMPORTANT!!!!
        // Ensure everything will be on disk.
        builder.close();

        System.out.println("Construct done. Elapsed: " + (System.currentTimeMillis() - start_t)
                / 1000.0 + "s");

        System.out.println("Reopen for verifing. Elapsed: "
                + (System.currentTimeMillis() - start_t) / 1000.0 + "s");

        // Re-open index to verify.
        builder = InvertedIndexBuilder.getBuilder(dir);
        IvtMapInteger reopenedMap = builder.getUnifiedDistributedIvtiIntegerMap();

        List<Integer> lst = reopenedMap.get(randPickToken);
        System.out.println("Reopen and retrive Posting List for token '" + randPickToken
                + "':");
        printList(lst);

        System.out.println("All done. Elapsed: " + (System.currentTimeMillis() - start_t)
                / 1000.0 + "s");
    }
}
