package PredictiveIndex;

import com.google.common.primitives.Ints;
import com.mchange.v1.util.ArrayUtils;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.apache.commons.lang.ObjectUtils;
import org.apache.poi.util.ArrayUtil;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;

import static PredictiveIndex.FastQueryTrace.getFQT;
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
    static IntOpenHashSet unigram = (IntOpenHashSet) deserialize(uniqueTerms);


    public static void getBigramIndex(String index) throws IOException, ClassNotFoundException {
        Int2ObjectOpenHashMap<int[][]> top1000I2 = new Int2ObjectOpenHashMap<>();

        System.out.println("Build bigram Inverted index...");
        DataInputStream DIStream = getDIStream(index);
        LinkedList<Integer> auxPostingList = new LinkedList<>();
        int[] posting = new int[3];                         //
        int currentTerm = -1;
        int [][] auxArray = null;
        //DataOutputStream DOStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(metadata+"PLLength.bin")));

        for(int term = 0, post = 0; true; ) {
            posting = getEntry(DIStream, posting);
            if (posting == null) break;

            if (posting[0] != currentTerm & (check | (checkTheCheck(posting[0])))){
                if(currentTerm != -1) {
                    top1000I2.put(currentTerm, Arrays.copyOf(auxArray, post));
                }
                currentTerm = posting[0];
                top1000I2.put(currentTerm, new int[1000][2]);
                auxArray = top1000I2.get(currentTerm );
                System.out.println((term++)+"-" + post);
                post=0;
            }if(post<1000 & (check | (checkTheCheck(posting[0])))){
                auxArray[post][0]   = posting[1];
                auxArray[post++][1] = posting[2];
                if(post==1000) check=false;
            }


        }
        int [] bA;
        int [][] aux = new int[2000][2];
        DataOutputStream DOS = getDOStream(bigramIndex);
        int intersectionLen;
        for (long bigram: (LongOpenHashSet) deserialize(smallFilterSet)){
            bA = getTerms(bigram);
            try {
                System.arraycopy(top1000I2.get(bA[0]), 0, aux, 0, top1000I2.get(bA[0]).length);
                System.arraycopy(top1000I2.get(bA[1]), 0, aux, top1000I2.get(bA[0]).length, top1000I2.get(bA[1]).length);
            }catch (NullPointerException e){
                System.out.println(bA[0]+"-"+top1000I2.get(bA[0]));
                System.out.println(bA[1]+"-"+top1000I2.get(bA[1]));

            }
            Arrays.parallelSort(aux, 0, top1000I2.get(bA[0]).length+top1000I2.get(bA[1]).length, c);
            intersectionLen = min(top1000I2.get(bA[0]).length+top1000I2.get(bA[1]).length, 1000);
            for(int i = 0; i< intersectionLen ; i++){
                DOS.writeLong(bigram);
                DOS.writeInt(aux[i][0]);
                DOS.writeInt(aux[i][1]);
            }
        }
        DOS.close();
    }

    private static boolean  checkTheCheck(int term){
        check = unigram.contains(term);
        return check;
    }

    //use train and query set togheter
    public static void checkFilterSets(){
        IntOpenHashSet  terms = (IntOpenHashSet) deserialize(uniqueTerms);
        LongOpenHashSet bigrams = (LongOpenHashSet) deserialize(smallFilterSet);
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
