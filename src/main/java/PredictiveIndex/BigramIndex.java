package PredictiveIndex;

import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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
    static IntOpenHashSet unigram = (IntOpenHashSet) deserialize(SMALL_FILTER_SET);

    /*The code has a strange outcome since in the final single inverted index we have just
    * 19752 posting list vs the 20856 terms. 1104 terms missing saved in a set
    *
    * X -I have found all the words in the corpora - in the whole corpora there is not such term.
    * 2) the threshold cuts out the lists
    * 3) other bugs
    *
    * */

    public static void getBigramIndex(String index) throws IOException, ClassNotFoundException {
        Int2ObjectOpenHashMap<int[][]> top1000I2 = new Int2ObjectOpenHashMap<>();

        System.out.println("Build bigram Inverted index...");
        DataInputStream DIStream = getDIStream(index);
        LinkedList<Integer> auxPostingList = new LinkedList<>();
        int[] posting = new int[3];                         //
        int currentTerm = -1;
        int [][] auxArray = null;
        byte [] toskip = new byte[4*3*100];
        //DataOutputStream DOStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(metadata+"PLLength.bin")));
        if(!checkExistence(UNIGRAMINDEX1000)) {
            for (int term = 0, post = 0; true; ) {
                posting = getEntry(DIStream, posting);
                if (posting == null) break;
                if (posting[0] != currentTerm) {
                    if (currentTerm != -1) {
                        top1000I2.put(currentTerm, Arrays.copyOf(auxArray, post));
                        //unigram.remove(currentTerm);
                        //System.out.println(Arrays.deepToString(top1000I2.get(currentTerm)));
                    }

                    currentTerm = posting[0];
                    top1000I2.put(currentTerm, new int[1000][2]);
                    auxArray = top1000I2.get(currentTerm);
                    System.out.println((term++) + "-" + post);
                    post = 0;
                }
                if (post < 1000) {
                    auxArray[post][0] = posting[1];
                    auxArray[post++][1] = posting[2];
                    if (post == 1000) check = false;
                }else{
                    DIStream.read(toskip);
                    if(currentTerm!=java.nio.ByteBuffer.wrap(Arrays.copyOfRange(toskip, 4*3*99, (4*3*99)+4)).getInt()) notToSkip(toskip);
                }


            }
            serialize(top1000I2, UNIGRAMINDEX1000);

        }else{
            top1000I2 = (Int2ObjectOpenHashMap<int[][]>) deserialize(UNIGRAMINDEX1000);
        }

        System.out.println(top1000I2.size());
        //System.out.println(top1000I2.containsKey(185));
        //serialize(unigram.removeAll(top1000I2.keySet()), results+"missingSet");
        //serialize(unigram, results+"missingSet");
        System.out.println(unigram.size());
        System.out.println(top1000I2.size());

        //System.exit(1);


        int [] bA;
        int [][] aux = new int[2000][2];
        DataOutputStream DOS = getDOStream(BIGRAMINDEX);
        int intersectionLen;
        int missing = 0;
        for (long bigram: (LongOpenHashSet) deserialize(SMALL_FILTER_SET)){
            bA = getTerms(bigram);
            try {
                System.arraycopy(top1000I2.get(bA[0]), 0, aux, 0, top1000I2.get(bA[0]).length);
                System.arraycopy(top1000I2.get(bA[1]), 0, aux, top1000I2.get(bA[0]).length, top1000I2.get(bA[1]).length);
                Arrays.parallelSort(aux, 0, top1000I2.get(bA[0]).length+top1000I2.get(bA[1]).length, c);
                intersectionLen = min(top1000I2.get(bA[0]).length+top1000I2.get(bA[1]).length, 1000);
            }catch (NullPointerException e){
                System.out.println(bA[0]+"-"+top1000I2.get(bA[0]));
                System.out.println(bA[1]+"-"+top1000I2.get(bA[1]));
                missing++;
                intersectionLen=0;

            }

            for(int i = 0; i< intersectionLen ; i++){
                DOS.writeLong(bigram);
                DOS.writeInt(aux[i][0]);
                DOS.writeInt(aux[i][1]);
            }
        }
        DOS.close();
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
        IntOpenHashSet  terms = (IntOpenHashSet) deserialize(SMALL_FILTER_SET);
        LongOpenHashSet bigrams = (LongOpenHashSet) deserialize(SMALL_FILTER_SET);
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
}
