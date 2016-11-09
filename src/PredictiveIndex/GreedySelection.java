package PredictiveIndex;

import it.unimi.dsi.fastutil.doubles.Double2LongRBTreeMap;
import it.unimi.dsi.fastutil.longs.Long2FloatOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

import java.io.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;

import static PredictiveIndex.QualityModel.getQualityModel;
import static PredictiveIndex.WWW.qualityModel;
import static PredictiveIndex.utilsClass.getPair;
import static PredictiveIndex.utilsClass.getTerms;

/**
 * Created by aalto on 10/1/16.
 */
public class GreedySelection extends Selection {

    static Long2IntOpenHashMap fetchPPLength(DataInputStream DIStream) throws IOException {
        Long2IntOpenHashMap PPLength = new Long2IntOpenHashMap();
        while(true){
            try{
                PPLength.put(DIStream.readLong(),DIStream.readInt());
            }catch (EOFException e ){
                return PPLength;
            }
        }
    }

    static void checkPairProbLen() throws IOException {
        if(!checkExistence(pairProbLen)) buildFinalModel();
    }

    static void checkPostingListLength(){
        if(!checkExistence(pListLength)) {
            System.out.println("Posting List Length Missing");
            System.exit(1);
        }
    }

    private static void buildFinalModel() throws IOException {
        System.out.println("Final Model Not Found. Building Final Model...");
        checkPostingListLength();
        Long2IntOpenHashMap PLLen = fetchPPLength(getDIStream(pListLength));
        BufferedReader br = getBuffReader(lanModel);
        DataOutputStream DOStream = getDOStream(pairProbLen);
        getTerm2IdMap();

        String line;
        long pair =0;
        String [] field;
        for (line = br.readLine(); line != null; line = br.readLine()){
            field = line.split(" ");
            try{
                pair = getPair(term2IdMap.get(field[0]),term2IdMap.get(field[1]));
                DOStream.writeLong(pair);
                DOStream.writeDouble(Double.valueOf(field[3]));
                DOStream.writeInt(PLLen.get(pair));
            }catch (NullPointerException e){
                System.out.println(term2IdMap.get(field[0])+","+term2IdMap.get(field[1]));
            }

        }
        br.close();
        DOStream.close();
    }


    static void greedySelection() throws IOException {
        Long2IntOpenHashMap pairMap = new Long2IntOpenHashMap();
        long pair = 0;
        float [] prob = new float[20000000];
        int [][] info = new int[20000000][2];                                        //length,pointer
        long [][] bucketOrder = (long [][]) deserialize(sortedRange);
        float [][] QM = (float[][]) deserialize(qualityModel);

        int [] lRanges = computelRanges(1.1);

        Double2LongRBTreeMap auxSelection = new Double2LongRBTreeMap();
        checkPairProbLen();
        DataInputStream DIStream = getDIStream(pairProbLen);
        for(int k = 0; true; k++){
            try{
                pair = DIStream.readLong();
                pairMap.put(pair,k);
                prob[k] = DIStream.readFloat();
                info[k][0] = DIStream.readInt();
                auxSelection.put(getScore(prob[k],info[k], QM,lRanges), pair);
                info[k][1]++;
            }catch (EOFException e){
                break;
            }
        }
        long maxBudget = 1000000;
        long budget = 0;
        LinkedList<long[]> finalModel = new LinkedList<>();
        long firstPair;
        int firstPointer;
        long range;

        while(budget < maxBudget){
            firstPair = auxSelection.remove(auxSelection.firstDoubleKey());
            firstPointer = pairMap.get(firstPair);
            //auxSelection.remove(auxSelection.firstDoubleKey());
            range = bucketOrder[info[firstPointer][0]][info[firstPointer][1]++];
            auxSelection.put(getScore(prob[firstPointer], info[firstPointer], QM, lRanges), pair);
            finalModel.addLast(new long[]{pair, range});
            budget += getRangeSize(range);

        }
        serialize(finalModel, toPick);
    }


    private static int getRangeSize(long range){
        return getTerms(range)[1]-getTerms(range)[0];
    }

    static double getScore(float prob, int[] info, float[][] qualityModel, int[] lRanges){
        if(info[0] < 43167165)  return qualityModel[getLenBucket(info[0], lRanges)][info[1]] * prob ;
        else return 0;
    }

}
