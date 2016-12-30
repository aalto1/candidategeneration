package PredictiveIndex;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by aalto on 12/30/16.
 */
public class SelectChuncks extends WWW {

    public static void selectBestChuncks(String input, String output, String metadata, String ranges2Select) throws IOException {
        String line;
        long [] data;
        int [] posting;
        DataInputStream DIS = getDIStream(input);
        BufferedReader br = getBuffReader(metadata);
        long [] aux = new long[30000000];
        DataOutputStream DOS = getDOStream(output);

        Long2ObjectOpenHashMap<long[]> map  = (Long2ObjectOpenHashMap<long[]>) deserialize(ranges2Select);
        int [] range;
        while((line = br.readLine())!=null){
            data = string2LongArray(line, " ");
            for (int i = 0; i < data[1]; i++)
                aux[i] = DIS.readLong();
            for (long e : map.get(Long.valueOf(data[0]).longValue())) {
                range = getTerms(e);
                

            }
        }

    }
}
