package PredictiveIndex;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.io.*;
import java.util.Arrays;

/**
 * Created by aalto on 9/11/16.
 */
class utilsClass {

    static int [] hashMapToArray(Int2IntMap map){
        /*This function convert an HashMap to an 1d array with the dimension doubled respect to the key-value pairs
        * with a value bigger than one*/

        int [] array = new int[map.size()*2];
        int value;
        int k = 0;
        for(int key : map.keySet()){
            value = map.get(key);
            if((value)>1){                          //becasue we want to reduce the overhead (20% space saved per dump)
                array[k*2] = key;
                array[k*2+1] = value;
                k++;
            }
        }
        return Arrays.copyOf(array, k*2+1);
    }

    static Int2IntMap arrayToHashMap(int [] array){
        /*This function converst an 1d array to an hashmap. We use this function to get the term freq. for a specific
        * term within a doc*/

        Int2IntMap map = new Int2IntOpenHashMap();
        for(int k = 0; k<array.length; k+=2){
            map.put(k, k+1);
        }
        return map;
    }

    static long getPair(int a , int b){
        return (long)a << 32 | b & 0xFFFFFFFFL;
    }

    static int[] getTerms(long c){
        int aBack = (int)(c >> 32);
        int bBack = (int)c;
        return new int[]{aBack,bBack};
    }

    static ObjectInputStream getOIStream(String filename, boolean buffered) throws IOException {
        ObjectInputStream OIStream;
        if(buffered) return new ObjectInputStream(new FileInputStream(filename));
        else return new ObjectInputStream( new BufferedInputStream(new FileInputStream(filename)));

    }

    static ObjectOutputStream getOOStream(String filename, boolean buffered) throws IOException {
        ObjectOutputStream IIStream;
        if(buffered) return new ObjectOutputStream(new FileOutputStream(filename));
        else return new ObjectOutputStream( new BufferedOutputStream(new FileOutputStream(filename)));
    }

    static void serialize(Object e, String filename) {
        try {
            ObjectOutputStream OOStream = getOOStream(filename, true);
            OOStream.writeObject(e);
            OOStream.close();
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    static Object deserialize(String file) {
        System.out.print("Fetching: " + file + "...");
        Object e = null;
        try {
            ObjectInputStream OIStream = getOIStream(file+".bin", true);
            e = OIStream.readObject();
            OIStream.close();
            System.out.println(" fetched!");
            return e;
        } catch (IOException i) {
            i.printStackTrace();
            return null;
        } catch (ClassNotFoundException c) {
            System.out.println("Object not found");
            c.printStackTrace();
            return null;
        }
    }
    /*528184109
    * #documnets: 50220423
    * rate: 10000000*/
    static void checkProgress(int p, int max, int rate, double start){
        if(p % rate == 0){
            int percentage = (int) (p*100.0)/max;
            System.out.println("Work in progress: " + percentage+ "%\tProcessing Time: " + (p / (System.currentTimeMillis() - start)) * 1000 + "doc/s. \tProcessed: " +p);
            System.out.println("Expected Remaining Time: "+ (((System.currentTimeMillis() - start)/percentage)*(7-percentage))/60000 + " minutes");
            //System.out.println("Expected time: " + (System.currentTimeMillis() - now)*(1/10*percentage));
        }
    }

    /*0xff = 255 = 11111111 = u-byte*/

    static int[] decodeRawDoc(byte[] rawDoc, int docLen) {
        int k = 0;
        int [] numbers = new int[docLen];
        int n = 0;
        for (byte b : rawDoc) {
            if ((b & 0xff) >= 128) {
                n = 128 * n + (b & 0xff);
            } else {
                int num = (128 * n + ((b - 128) & 0xff));
                numbers[k] = num;
                if(num<0) System.out.println(num);
                k++;
                n = 0;
            }
        }
        return numbers;
    }


}
