package PredictiveIndex;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by aalto on 12/7/16.
 */
public class Tests extends WWWMain{

    public static void december7() throws IOException {
        Int2IntOpenHashMap map = (Int2IntOpenHashMap) deserialize(localFreqMap);
        int [] a = new int[map.size()];
        map.values().toArray(a);
        Arrays.sort(a);
        System.out.println(a.length);
        String result = Arrays.toString(Arrays.copyOf(a,10000));
        System.out.println(a[9999]);
        BufferedWriter bw = getBuffWriter("/home/aalto/IdeaProjects/PredictiveIndex/data/test/dec7");
        bw.write(result.substring(1, result.length()-2));
        bw.close();
        /*for (int a : map.keySet()) {
            System.out.println(a + " - " + map.get(a));
        }*/
        System.exit(1);
    }
}
