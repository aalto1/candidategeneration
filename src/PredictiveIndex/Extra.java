package PredictiveIndex;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.Object2ByteOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.lemurproject.kstem.KrovetzStemmer;

import java.io.*;
import java.util.LinkedList;

/**
 * Created by aalto on 10/1/16.
 */
public class Extra extends WWW {

    public static void uniquePairs() throws IOException {
        getTermMap();
        Long2IntOpenHashMap accMap = new Long2IntOpenHashMap();
        BufferedReader br = new BufferedReader(new FileReader(trainQ));
        String line;
        String [] field;
        Integer term;
        LinkedList<Integer> terms = new LinkedList<>();
        KrovetzStemmer stemmer = new KrovetzStemmer();

        for (line = br.readLine(); line != null; line = br.readLine()) {
            field = line.split(":")[1].split(" ");

            for (int i = 0; i < field.length; i++) {
                term = termMap.get(field[i]);
                if (term != null) {
                    terms.addLast(term);
                    if (accMap.putIfAbsent(Long.valueOf(term), 1) != null)
                        accMap.merge(Long.valueOf(term), 1, Integer::sum);
                }
            }
            for (long i : getCombinations(terms, 2)) {
                if (accMap.putIfAbsent(i, 1) != null) accMap.merge(i, 1, Integer::sum);
            }
            terms.clear();
        }
        serialize(accMap, accessMap);

    }

    private static int[] getQueryInts(String[] queryTerms) {
        int[] queryInt = new int[queryTerms.length];
        for (int i = 0; i < queryTerms.length; i++) queryInt[i] = termMap.get(queryTerms[i]);
        return queryInt;
    }

    private static int[] getQueryTopkDocIDs(String[] topk) {
        //declare a new object every time
        int[] topkInt = new int[topk.length];

        // We convert our String [] to int [] using the term-termID map
        for (int i = 0; i < topk.length; i++) {
            topkInt[i] = 1; //docIDmap.get(String);
        }
        return topkInt;
    }

    static void  getFilterSet() throws IOException {
        getTermMap();
        LongOpenHashSet fSet = new LongOpenHashSet();
        BufferedReader br = getBuffReader(lanModel);
        String line;
        String [] field;
        int t1, t2, removed = 0;
        for(line = br.readLine(); line!=null; line = br.readLine()){
            field = line.split(" ");
            try{
                t1 = termMap.get(field[0]);
                t2 = termMap.get(field[1]);
                fSet.add(getPair(t1,t2));
            }catch (NullPointerException e){
                removed++;
            }

        }
        System.out.println("Absent Terms: " + removed + " removed.");
        System.out.println("Filter Set Size: " + fSet.size());
        serialize(fSet, filterSet);
    }

    static void getDocIDMap() throws IOException {
        BufferedReader br = getBuffReader(didNameMap);
        Object2IntOpenHashMap<String> doc2IDmap = new Object2IntOpenHashMap<>();
        String line;
        String [] field;
        System.out.println("Fetching didNameMap...");
        for (line = br.readLine(); line != null ; line = br.readLine()) {
            field = line.split(" ");
            doc2IDmap.put(field[1], Integer.valueOf(field[0]).intValue());
        }
        br.close();
        br = getBuffReader(oldDocInfo);
        BufferedWriter bw = getBuffWriter(finalDocInfo);
        System.out.println("Fetching oldDocInfo and merging the two in finalDocInfo...");
        for (line = br.readLine(); line != null ; line = br.readLine()) {
            field = line.split(" ");
            bw.write(field[0] + " " + doc2IDmap.get(field[0]) + " " + field[2] + " " + field[3] + " " + field[4] + "\n");
        }
        bw.close();
        br.close();
        System.out.println("Merging Completed");

    }
}