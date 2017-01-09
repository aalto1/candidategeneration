package PredictiveIndex;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

/**
 * Created by aalto on 11/23/16.
 */
public class NewQualityModel extends Selection {
    static Long2IntOpenHashMap accMap;
    static Int2LongOpenHashMap dumped;
    static LongOpenHashSet hitPairs = new LongOpenHashSet();
    static long hit = 0;


    /**Once we process a process a posting list the corrisponding array is completely filled up. The only
     * thing that could change this is a line that you did not process the whole corpora
     *
     * negative number => full index can have negative bm25
     * check score of the first one => always bigger
     * inverted index not sorted => now sorted
     * */

    public static double[][][] getModel2(String index, String output, String length, int fields) throws IOException, ClassNotFoundException {
        System.out.println("Fast Query Trace fetched!\n Processing Inverted Index...");
        Long2ObjectOpenHashMap<Int2ObjectOpenHashMap<Int2IntOpenHashMap>> referenceModel =
                (Long2ObjectOpenHashMap<Int2ObjectOpenHashMap<Int2IntOpenHashMap>>) deserialize(REFERENCEMODEL);
        Int2ObjectOpenHashMap<Long2ObjectOpenHashMap<long[]>> emptymodel = (Int2ObjectOpenHashMap<Long2ObjectOpenHashMap<long[]>>) deserialize(EMPTYMODEL);
        dumped = (Int2LongOpenHashMap) deserialize(length);

        DataInputStream DIStream = getDIStream(index);
        int[] posting = new int[fields];
        long currentTerm;
        long currentPostingList = -1;
        Int2ObjectOpenHashMap<Int2IntOpenHashMap> documentsToFind = new Int2ObjectOpenHashMap();
        Int2IntOpenHashMap scores;
        int counter = 0;
        int postingNumber = 0;

        while (true) {
            if ((posting = Selection.getEntry(DIStream, posting)) == null) break;

            if(fields == 3)
                currentTerm = posting[0];
            else
                currentTerm = getPair(posting[0], posting[1]);

            if (currentTerm != currentPostingList) {
                currentPostingList = currentTerm;
                documentsToFind = referenceModel.get(posting[0]);
                postingNumber += counter;
                counter = 0;
            }
            try {
                if (documentsToFind.size() > 0 & (scores = documentsToFind.remove(posting[2])) != null) {
                    for (int qID : scores.keySet()) {
                        try {
                            emptymodel.get(qID).get(currentTerm)[scores.get(qID)] = counter; //getPair(posting[2], counter);
                             //printResult(emptymodel.get(qID), qID, newBM25, posting[1], scores.get(qID), currentTerm);
                        }catch (Exception e) {
                            //System.err.println(e.getMessage());
                        }
                    }
                }
            }catch(Exception e){
                //System.out.println(Arrays.toString(posting));
            }
            counter++;

        }
        System.out.println(hitPairs.size());
        System.out.println(postingNumber);
        serialize(emptymodel, output);
        //printQualityModel(model);
        return QM;
    }

    public static void getModel(String input, String output, String metadata, String filterSet) throws IOException {
        String line;
        long [] data;
        int [] posting;
        Int2IntOpenHashMap scores;
        DataInputStream DIS = getDIStream(input);
        LongOpenHashSet smallFilterSet = (LongOpenHashSet) deserialize(filterSet) ;
        BufferedReader br = getBuffReader(metadata);
        Int2ObjectOpenHashMap<Int2IntOpenHashMap> documentsToFind;

        Long2ObjectOpenHashMap<Int2ObjectOpenHashMap<Int2IntOpenHashMap>> referenceModel =
                (Long2ObjectOpenHashMap<Int2ObjectOpenHashMap<Int2IntOpenHashMap>>) deserialize(REFERENCEMODEL);
        Int2ObjectOpenHashMap<Long2ObjectOpenHashMap<long[]>> emptymodel = (Int2ObjectOpenHashMap<Long2ObjectOpenHashMap<long[]>>) deserialize(EMPTYMODEL);

        while((line = br.readLine())!=null){

            if(line.equals(CHUNKSIZE)){
                line = br.readLine();
                if(line == null){
                    break;
                }
            }

            data = string2LongArray(line, " ");
            if(true/*smallFilterSet.contains(data[0])*/){                       //THIS SHOULD BE REMOVED IN THE NEXT ITERATIONS
                documentsToFind = referenceModel.get(data[0]);
                System.out.println(referenceModel.containsKey(data[0]) + " " + data[0]);
                for (int i = 0; i < data[1]; i++) {
                    posting = getTerms(DIS.readLong());
                    if (documentsToFind.size() > 0 & (scores = documentsToFind.remove(posting[0])) != null) {
                        for (int qID : scores.keySet()) {
                            try {
                                emptymodel.get(qID).get(data[0])[scores.get(qID)] = i;
                                //printResult(emptymodel.get(qID), qID, newBM25, posting[1], scores.get(qID), currentTerm);
                            }catch (Exception e) {
                                //System.err.println(e.getMessage());
                            }
                        }
                    }

                }
            }
        }
        serialize(emptymodel,output);
    }


    public static void printResult(Long2ObjectOpenHashMap<long[]> m, int qID, int topBm25, int bm25, int pos, long term){
        System.out.println(qID);
        for(long [] a : m.values()) {
            for (long i : a) {
                System.out.print(Arrays.toString(getTerms(i)));
            }
            System.out.println(" " + dumped.get((int)term));
            //System.err.println("top: "+topBm25+" elem: "+bm25+ " diff: " + (topBm25-bm25) + " position: " + pos);
            //break;
        }
    }


    public static void buildQualityMatrix(String input, String output) throws IOException {
        double [][] finMod = new double[QM.length][QM[0].length];
        Long2IntOpenHashMap lenMap = (Long2IntOpenHashMap) deserialize(LOCALTERMFREQMAP);
        accMap = (Long2IntOpenHashMap) deserialize(ACCESSMAP);
        double value = 0;
        int x,y;
        int [] gapExtremes;
        Int2ObjectOpenHashMap<Long2ObjectOpenHashMap<long[]>> model = (Int2ObjectOpenHashMap<Long2ObjectOpenHashMap<long[]>>) deserialize(input);

        for (Long2ObjectOpenHashMap<long[]> aguTerms: model.values()) {
            for(long aguTerm: aguTerms.keySet()){
                x = getLenBucket(lenMap.get((int)aguTerm));
                //System.out.println(lenMap.get((int)aguTerm) + " " + x);
                for (long score: aguTerms.get(aguTerm)) {
                    y = getRankBucket(0, (int) score);
                    QM[x][y][0]++;
                }
            }
        }
        for (long aguTerm:accMap.keySet()) {
            x = getLenBucket(lenMap.get((int)aguTerm));
            y = rRanges.length-1;
            for (int k = 0; k < y ; k++) {
                gapExtremes = getTerms(deltaRanges[k]);
                QM[x][k][1] += accMap.get((int)aguTerm)*(gapExtremes[1]-gapExtremes[0]);
            }
        }

        for (int i = 0; i < QM.length; i++) {
            for (int j = 0; j < QM[0].length; j++) {
                QM[i][j][0] = (QM[i][j][0]*1.0)/(QM[i][j][1]);
                QM[i][j][1] = j;
            }
        }

        for (double[][] aQM : QM)
            Arrays.sort(aQM, (int1, int2) -> Double.compare(int2[0], int1[0]));


        for (double[][] xQM : QM){
            for (double [] yQM : xQM) {
                System.out.print(Arrays.toString(yQM));
            }
            System.out.println();
        }
        serialize(QM,output);

    }

    public static void generateMatrixModels() throws IOException {
        buildQualityMatrix(FILLEDUNIGRAM, UNIGRAMQUALITYMODEL);
        System.out.println("\n\n\n\n");
        buildQualityMatrix(FILLEDHIT, HITQUALITYMODEL);
        System.out.println("\n\n\n\n");
        buildQualityMatrix(FILLEDDBIGRAM, DBIGRAMQUALITYMODEL);
        System.out.println("\n\n\n\n");
        buildQualityMatrix(FILLEDBIGRAM, BIGRAMQUALITYMODEL);
        System.out.println("\n\n\n\n");

    }









}
