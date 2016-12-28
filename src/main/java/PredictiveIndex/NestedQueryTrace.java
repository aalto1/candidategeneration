package PredictiveIndex;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static PredictiveIndex.utilsClass.*;

/**
 * Created by aalto on 11/22/16.
 */
public class NestedQueryTrace extends Selection{


    /*
    * Empty Model
    * QID   Pair    topK
    * Int   Long    Array
    *
    * Total train q: 29186 queryId
    * Unigram model: 29167 queryId
    * Bigram model : 28660 queryId
    *
    *
    * Nine duplicates found:
        Duplicate: 115988
        Duplicate: 141101
        Duplicate: 15712
        Duplicate: 164894
        Duplicate: 112071
        Duplicate: 25906
        Duplicate: 67401
        Duplicate: 94213
        Duplicate: 83439
    *
    * */




    /*
    * Reformatted Query Trace
    * Long  Int     Int    Int
    * Pair  DocID   QID    rank
    *
    *
    * Final Structure
    * QID   aguTerm    topK
    * Int   Long    Array
    *
    * */

    /*
    * Stopwords are removed from the query trace: How, on, in...and similar terms are removed.
    * Stopword queries are removed, i.e., 173261. A total of: 10
    *
    * Some queries miss in the complex ranker 57287 (440 ranking misses)
    *
    * */

    public static void getEmptyModel(String inputQTrace, String outputModel) throws IOException {
        BufferedReader br = getBuffReader(inputQTrace);
        Int2ObjectOpenHashMap<Long2ObjectOpenHashMap<long[]>> emptyMod = new Int2ObjectOpenHashMap<>();
        Long2ObjectOpenHashMap<long[]> queryModel;
        String line;
        String [] fields;
        while((line=br.readLine())!= null){
            fields = line.split(":");
            queryModel = new Long2ObjectOpenHashMap<>();
            for(long aguTerm : string2LongArray(fields[1], " ")){
                queryModel.put(aguTerm, new long[10]);
            }
            if(emptyMod.put(Integer.valueOf(fields[0]).intValue(), queryModel)!=null){
                System.out.println("Duplicate: " + Integer.valueOf(fields[0]));
            }
        }
        System.out.println("Empty model built: " + outputModel + ". Size: " + emptyMod.size());
        serialize(emptyMod, outputModel);
        br.close();
    }

    private static int[][] getTopDoc(int[][] topMatrix, BufferedReader br) throws IOException {
        System.out.println("Building TopK matrix...");
        String line;
        int [] array;
        while ((line = br.readLine()) != null) {
            array = string2IntArray(line, ",");
            topMatrix[array[0]] = Arrays.copyOfRange(array, 1, array.length);
        }
        System.out.println("TopK matrix built.");
        return topMatrix;
    }

    static Long2ObjectOpenHashMap<Int2ObjectOpenHashMap<Int2IntOpenHashMap>> reference;

    public static Long2ObjectOpenHashMap<Int2ObjectOpenHashMap<Int2IntOpenHashMap>> buildReference(String input, String output) throws IOException {
        int[][] topKMatrix = getTopDoc(new int[173800][], getBuffReader(COMPLEXRANKERTOP));
        IntOpenHashSet missingQueries = new IntOpenHashSet();
        reference = new Long2ObjectOpenHashMap<>();
        BufferedReader br= getBuffReader(input);
        String line;
        String [] field;
        long[] aguTerms;
        int counterFail=0;

        while ((line = br.readLine()) != null) {
            field = line.split(":");
            aguTerms = string2LongArray(field[1], " ");
            for (long bigram : aguTerms) {
                if(topKMatrix[Integer.valueOf(field[0])]!=null)
                    addTopList(bigram, Integer.valueOf(field[0]),topKMatrix[Integer.valueOf(field[0])]);
                else{
                    System.out.println(line+ " " + ++counterFail);
                    missingQueries.add(Integer.valueOf(field[0]));
                    break;

                }
            }
        }
        serialize(reference, output);
        //serialize(missingQueries,MISSINGQUERIES);
        //System.out.println(reference.get(1).size());
        //System.out.println(reference.size());

        return reference;
    }

    public static void addTopList(long aguTerm, int qID, int [] topDocs){
        Int2ObjectOpenHashMap<Int2IntOpenHashMap> pairDocMap;
        Int2IntOpenHashMap docQueryMap;
        if((pairDocMap = reference.get(aguTerm))==null)
            pairDocMap = new Int2ObjectOpenHashMap<>();

        for (int i = 0; i < topDocs.length ; i++) {
            if((docQueryMap = pairDocMap.get(topDocs[i]))==null)
                docQueryMap = new Int2IntOpenHashMap();
            docQueryMap.put(qID, i);
            pairDocMap.put(topDocs[i], docQueryMap);
        }
        reference.put(aguTerm, pairDocMap);
    }















}
