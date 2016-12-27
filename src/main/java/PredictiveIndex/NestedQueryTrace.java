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
                    System.out.println(Integer.valueOf(field[0]) + " " + ++counterFail);
                    break;

                }
            }
        }
        serialize(reference, output);
        System.out.println(reference.get(1).size());
        System.out.println(reference.size());
        System.exit(1);
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
