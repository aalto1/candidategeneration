package PredictiveIndex;

import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.*;
import java.util.LinkedList;

/**
 * Created by aalto on 12/26/16.
 */
public class Metadata extends WWW {

    /**
     * This class is collects static methods to collect and build metadata informations about the dataset  */

    public static Long2DoubleOpenHashMap getUnigramLanguageModel() throws IOException {
        if(!checkExistence(UNIGRAMLANGUAGEMODELMAPPING)){
            if(!checkExistence(UNIGRAMLANGUAGEMODELCONVERTED)){
                convertLanguageModel(UNIGRAMLANGUAGEMODEL, UNIGRAMLANGUAGEMODELCONVERTED, "unigram");
            }return buildLanguageMap(UNIGRAMLANGUAGEMODELCONVERTED, UNIGRAMLANGUAGEMODELMAPPING);
        }else{
            return (Long2DoubleOpenHashMap) deserialize(UNIGRAMLANGUAGEMODELMAPPING);
        }
    }

    public static Long2DoubleOpenHashMap getBigramLanguageModel() throws IOException {
        if(!checkExistence(BIGRAMLANGUAGEMODELMAPPING)){
            if(!checkExistence(BIGRAMLANGUAGEMODELCONVERTED)){
                convertLanguageModel(BIGRAMLANGUAGEMODEL, BIGRAMLANGUAGEMODELCONVERTED, "bigram");
            }return buildLanguageMap(BIGRAMLANGUAGEMODELCONVERTED, BIGRAMLANGUAGEMODELMAPPING);
        }else{
            return (Long2DoubleOpenHashMap) deserialize(BIGRAMLANGUAGEMODELMAPPING);
        }
    }

    private static Long2DoubleOpenHashMap buildLanguageMap(String input, String output) throws IOException {
        Long2DoubleOpenHashMap map = new Long2DoubleOpenHashMap();
        BufferedReader br = getBuffReader(input);
        String line;
        String [] entry;
        int k = 0;
        while((line=br.readLine())!=null){
            try{
                entry = line.split(" ");
                map.put(Long.valueOf(entry[0]).longValue(),Double.valueOf(entry[1]).doubleValue());
            }catch (NumberFormatException e){
                k++;
            }
        }
        System.out.println("Counter: " + k + ". Map size: " + map.size());
        br.close();
        serialize(map, output );
        return map;
    }

    private static void convertLanguageModel(String input, String output, String type) throws IOException {
        getTerm2IdMap();
        BufferedReader br = getBuffReader(input);
        BufferedWriter bw = getBuffWriter(output);
        String line;
        String [] entry;
        while((line=br.readLine())!=null){
            entry = line.split(" ");
            try {
                switch (type) {
                    case "bigram":
                        bw.write(getPair(term2IdMap.get(entry[0]), term2IdMap.get(entry[1])) + " " + entry[3] + "\n");
                        break;
                    case "unigram":
                        bw.write(term2IdMap.get(entry[0]) + " " + entry[2] + "\n");
                        break;
                }
            }catch (NullPointerException e){

            }
        }
        br.close();
        bw.close();
    }


    /*SMALLFILTERSET TAKES INTO ACCOUNT JUST THE TERMS THAT ARE PRESENT IN THE QUERYTRACE*/

    static LongOpenHashSet getSmallFilterSet(String input, String output) throws IOException {
        LongOpenHashSet filterSet;
        if(!checkExistence(output)){
            filterSet = new LongOpenHashSet();
            BufferedReader br = getBuffReader(input);
            String line;
            long[] fields;
            int removed = 0;
            for (line = br.readLine(); line != null; line = br.readLine()) {
                fields = string2LongArray(line.split(":")[1], " ");
                try {
                    for (long field : fields) {
                        filterSet.add(field);
                    }
                } catch (NullPointerException e) {
                    removed++;
                }

            }
            System.out.println("Absent Terms: " + removed + " removed.");
            System.out.println("Filter Set Size: " + filterSet.size());
            serialize(filterSet, output);
        }else{
            filterSet = (LongOpenHashSet) deserialize(output);
        }
        return filterSet;
    }

    /***
     * TrainQ   = 58513
     * TestQ    = 5816
     * TotQ     = 64329
     *
     */

    /** Scanning the UNIGRAMLANGUAGEMODEL and BIGRAMLANGAUGE models build a set which contains all the terma and
     * bigrams that can potentially appear with a probability of a certain treshold*/

    public static LongOpenHashSet getBigFilterSet(String [] inputs, String output) throws IOException {
        LongOpenHashSet filterSet;
        if(!checkExistence(output)) {
            filterSet = new LongOpenHashSet();
            for (String input : inputs) {
                BufferedReader br = getBuffReader(input);
                String line;
                String[] fields;
                for (line = br.readLine(); line != null; line = br.readLine()) {
                    fields = line.split(" ");
                    filterSet.add(Long.valueOf(fields[0]).longValue());
                }
                br.close();
            }
        }else{
            filterSet = (LongOpenHashSet) deserialize(output);
        }
        System.out.println("Big Filter Size: " + filterSet.size());
        serialize(filterSet, output);
        return filterSet;

    }

    public static Long2IntOpenHashMap getAccessMap(String input, String output) throws IOException {
        Long2IntOpenHashMap map = new Long2IntOpenHashMap();
        BufferedReader br = getBuffReader(input);
        String line;
        long [] fields;
        while((line=br.readLine())!=null){
            for (long term: string2LongArray(line.split(":")[1], " ")) {
                if(map.putIfAbsent(term,1)!=null){
                    map.merge(term,1,Integer::sum);
                }
            }
        }
        System.out.println("Map size: " + map.size());
        br.close();
        serialize(map, output );
        return map;


    }







}
