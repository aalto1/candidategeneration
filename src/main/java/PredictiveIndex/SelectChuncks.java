package PredictiveIndex;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.*;
import java.util.ArrayList;

/**
 * Created by aalto on 12/30/16.
 */
public class SelectChuncks extends WWW {

    public static void getBestChuncks(String input, String inputMeta, String output, String outMeta, String ranges2Select) throws IOException {
        String line;
        long [] data;
        int [] posting;
        DataInputStream DIS = getDIStream(input);
        BufferedReader br = getBuffReader(inputMeta);
        long [] aux = new long[30000000];
        DataOutputStream DOS = getDOStream(output);
        BufferedWriter bw = getBuffWriter(outMeta);
        int counter = 0;
        int globalCounter = 0;
        Long2ObjectOpenHashMap<ArrayList<Long>> map  = (Long2ObjectOpenHashMap<ArrayList<Long>>) deserialize(ranges2Select);

        int [] range;
        while((line = br.readLine())!=null){
            data = string2LongArray(line, " ");
            for (int i = 0; i < data[1]; i++)
                aux[i] = DIS.readLong();
            try{
                for (long e : map.get(Long.valueOf(data[0]).longValue())) {
                    range = getTerms(e);
                    for (int i = range[0]; i < range[1] ; i++) {
                        DOS.writeLong(aux[i]);
                        counter++;
                    }
                }
                globalCounter += counter;
                bw.write(data[0] + " " + counter + " " + globalCounter + "\n");
                counter = 0;

            }catch (NullPointerException e){
                //System.out.println("No chuncks of this posting list have been selected by the greedy algorithm");
            }

        }
        bw.close();
        DOS.close();
    }
}
