package PredictiveIndex;

import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;

import static PredictiveIndex.Selection.deltaRanges;
import static PredictiveIndex.Selection.getEntry;
import static PredictiveIndex.WWW.*;
import static java.lang.Integer.min;

/**
 * Created by aalto on 11/15/16.
 */
public class BigramIndex {

    static Comparator<int[]> c = new Comparator<int[]>() {
        @Override
        public int compare(int[] int1, int[] int2) {
            return Integer.compare(int1[1], int2[1]);
        }
    };

    static boolean check= false;
    static LongOpenHashSet unigram = (LongOpenHashSet) deserialize(UNIGRAM_SMALL_FILTER_SET);

    /*The code has a strange outcome since in the final single inverted index we have just
    * 19752 posting list vs the 20856 terms. 1104 terms missing saved in a set
    *
    * X -I have found all the words in the corpora - in the whole corpora there is not such term.
    * 2) the threshold cuts out the lists
    * 3) other bugs
    *
    * */

    public static void getBigramIndex(String index, int budget) throws IOException, ClassNotFoundException {
        Long2ObjectOpenHashMap<long[]> top1000I2;
        System.out.println("Build bigram Inverted index...");
        Long2IntOpenHashMap bigramPostLen = new Long2IntOpenHashMap();
        if(!checkExistence(UNIGRAMTOPMAP))
            top1000I2 = getUnigramTopMap(UNIGRAMINDEX,UNIGRAMTOPMAP, budget);
        else
            top1000I2 = (Long2ObjectOpenHashMap<long[]>) deserialize(UNIGRAMTOPMAP);
        BufferedWriter bw = getBuffWriter(BIGRAMMETA);


        int [] bA;
        long [] aux = new long[budget*2];
        int intersectionLen;
        DataOutputStream DOS = getDOStream(BIGRAMINDEX);
        int missing = 0;
        for (long bigram: (LongOpenHashSet) deserialize(BIGRAM_SMALL_FILTER_SET)){
            bA = getTerms(bigram);
            try {
                System.arraycopy(top1000I2.get(bA[0]), 0, aux, 0, top1000I2.get(bA[0]).length);
                System.arraycopy(top1000I2.get(bA[1]), 0, aux, top1000I2.get(bA[0]).length, top1000I2.get(bA[1]).length);

                Arrays.parallelSort(aux, 0, top1000I2.get(bA[0]).length+top1000I2.get(bA[1]).length);
                intersectionLen = min(top1000I2.get(bA[0]).length+top1000I2.get(bA[1]).length, budget);
            }catch (NullPointerException e){
                System.out.println(bA[0]+"-"+top1000I2.get(bA[0]));
                System.out.println(bA[1]+"-"+top1000I2.get(bA[1]));
                missing++;
                intersectionLen=0;

            }
            bw.write(bigram + " " + intersectionLen + "\n");
            bigramPostLen.put(bigram, intersectionLen);
            for(int i = 0; i< intersectionLen ; i++){
                DOS.writeLong(aux[i]);
            }
        }
        DOS.close();
        bw.close();
        serialize(bigramPostLen, BILENGTHS);
        System.out.println(missing);
    }

    private static void notToSkip(byte [] noSkip){
        java.nio.ByteBuffer.wrap(Arrays.copyOfRange(noSkip, 4*3*99, (4*3*99)+4)).getInt();
        for (int i = 0; i < noSkip.length; i+=4) {
            java.nio.ByteBuffer.wrap(Arrays.copyOfRange(noSkip, i, i+4)).getInt();
            java.nio.ByteBuffer.wrap(Arrays.copyOfRange(noSkip, i+4, i+8)).getInt();
            java.nio.ByteBuffer.wrap(Arrays.copyOfRange(noSkip, i+8, i+12)).getInt();
        }

    }

    private static boolean  checkTheCheck(int term){
        check = unigram.contains(term);
        return check;
    }

    //use train and query set togheter
    public static void checkFilterSets(){
        LongOpenHashSet  terms = (LongOpenHashSet) deserialize(UNIGRAM_SMALL_FILTER_SET);
        LongOpenHashSet bigrams = (LongOpenHashSet) deserialize(BIGRAM_SMALL_FILTER_SET);
        int [] aux;
        int counter=0;
        for(long bigram : bigrams){
            aux = getTerms(bigram);

            if(!terms.contains(aux[0])){
                System.out.println(aux[0]);
                counter++;
            }
            if(!terms.contains(aux[1])){
                System.out.println(aux[1]);
                counter++;
            }
        }
        System.out.println("\n"+counter);
        System.exit(1);
    }



    public static Long2ObjectOpenHashMap<long[]> getUnigramTopMap(String input, String output, int budget) throws IOException {
        String line;
        long [] data;
        long posting;
        long [] top = new long[budget];
        Long2ObjectOpenHashMap<long[]> unigramTopMap = new Long2ObjectOpenHashMap<>();
        DataInputStream DIS = getDIStream(input);
        LongOpenHashSet smallFilterSet = (LongOpenHashSet) deserialize(UNIGRAM_SMALL_FILTER_SET) ;
        BufferedReader br = getBuffReader(UNIGRAMMETA);

        while((line = br.readLine())!=null){
            data = string2LongArray(line, " ");
            if(smallFilterSet.contains(data[0])){                       //THIS SHOULD BE REMOVED IN THE NEXT ITERATIONS
                for (int i = 0; i < data[1]; i++) {
                    posting = DIS.readLong();
                    if(i<budget){
                        top[i] = posting;
                    }else if(i==budget){
                        unigramTopMap.put(posting, top);
                    }
                }
            }
        }
        serialize(unigramTopMap,output);
        return unigramTopMap;
    }
}
