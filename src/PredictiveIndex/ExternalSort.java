package PredictiveIndex;

import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.io.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.ListIterator;

/**
 * Created by aalto on 9/12/16.
 */
public class ExternalSort {
    static long [][] bucket;
    private static long now = System.currentTimeMillis();
    static long partialNow;
    static Comparator<long[]> comp = new Comparator<long[]>() {
        @Override
        public int compare(long[] int1, long[] int2) {
            //if we have the same doc ids sort them based on the bm25
            if (int1[0] == int2[0]) {
                return Long.compare(int2[1], int1[1]);
            } else return Long.compare(int1[0], int2[0]);
        }
    };

    static long maxData = Long.MAX_VALUE;

    public static void binaryMassiveSort(String input, String output, int recordsNumber) throws IOException {
        now = System.currentTimeMillis();
        int p=0;
        DataInputStream DIStream = new DataInputStream(new BufferedInputStream(new FileInputStream(input)));
        System.out.print("Allocating space...\t");
        partialNow = System.currentTimeMillis();
        bucket = new long[250000000][2];
        System.out.println("done: " + (System.currentTimeMillis() - partialNow) / 1000 + "s");
        DataOutputStream DOStream;
        for (int k = 1; true; k++) {
            DOStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(output + k)));
            System.out.print("Loading data...");
            partialNow = System.currentTimeMillis();
            try {
                for (p = 0; p < bucket.length; p++) {
                    bucket[p][0] = DIStream.readLong();
                    bucket[p][1] = DIStream.readLong();
                }
            } catch (EOFException e) {
                routine(DOStream, p+1);
                break;
            }
            routine(DOStream, bucket.length);
        }
        System.out.print((System.currentTimeMillis() - now)/60000+"min");
    }

        static void routine(DataOutputStream DOStream, int p) throws IOException {
            System.out.println("done: " + (System.currentTimeMillis() - partialNow)/1000 + "s");
            System.out.print("Sorting...\t");
            partialNow = System.currentTimeMillis();
            java.util.Arrays.parallelSort(bucket, 0, p, comp);
            System.out.println("done: " + (System.currentTimeMillis() - partialNow)/1000 + "s");
            System.out.print("Writing on disk...\t");
            partialNow = System.currentTimeMillis();
            for(int x =0; x<p; x++) for (long entry: bucket[x]) DOStream.writeLong(entry);
            DOStream.close();
            System.out.println("done: " + (System.currentTimeMillis() - partialNow)/1000 + "s");
    }

    /*public static void readFiles(File folder, String output) throws IOException {
        long [][][] bucketsA;
        long [][][] bucketsB;
        File [] files = folder.listFiles();
        DataInputStream [] DIStreams = new DataInputStream[files.length];
        DataOutputStream DOStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(output)));
        int section =  250000000/(files.length+2);
        bucketsA = new long[files.length][section][2];
        for(int k =0; k< files.length; k++){
            DIStreams[k] =  new DataInputStream(new BufferedInputStream(new FileInputStream(files[k])));
        }
        for(int y = 0; y< DIStreams.length; y++){
            for(int z = 0; z < section; z++){
                bucketsA[y][z][0] = DIStreams[y].readLong() ;
                bucketsA[y][z][1] = DIStreams[y].readLong() ;
            }
        }
        for (int x = files.length/2; bucketsA.length>2 ; x /=2) {
            System.out.println("Merging " + x);
            bucketsB = new long[x][section][2];
            for (int i = 0; i < bucketsB.length ; i++) {
                System.out.println("Merging " + x +"-"+i);
                bucketsB[i] = mergeSortedMatrix(bucketsA[(i*2)], bucketsA[(i*2)+1]);
                bucketsA[(i*2)]   = null;
                bucketsA[(i*2)+1] = null;
            }
            x/=2;
            if(bucketsB.length==2){
                writeMergeSortedMatrix(bucketsB[0], bucketsB[1], DOStream);
                break;
            }
            bucketsA = new long[x][section][2];
            for (int i = 0; i < bucketsB.length ; i++) {
                System.out.println("Merging:" + x +"-"+i);
                bucketsA[i] = mergeSortedMatrix(bucketsB[(i*2)], bucketsB[(i*2)+1],);
                bucketsB[(i*2)]   = null;
                bucketsB[(i*2)+1] = null;
            }
        }
        writeMergeSortedMatrix(bucketsA[0], bucketsA[1], DOStream);


    }*/

    public static long[][] mergeSortedMatrix(long[][] a, long[][] b, int size, int step) {
        long [][] answer = new long[a.length + b.length][2];
        int i = 0, j = 0, k = 0;
        //if(step == 0) for (long  [] z : a) System.out.print(z[1]);

        //if(size < 5 & k%100000 == 0) for(long z[]:a) System.out.println(z[0]);
        while (i < a.length && j < b.length) {
            if (comp.compare(a[i], b[j])<0)
                answer[k++] = a[i++];
            else
                answer[k++] = b[j++];
        }
        System.arraycopy(a, i, answer, k, (a.length -i));
        System.arraycopy(b, j, answer, k, (b.length -j));
        //if(step == 1) for (long  [] z : answer) System.out.print(z[1]);
        //System.out.println("Last Element: " + answer[answer.length-1][0]);
        return answer;
    }



    public static void massiveBinaryMerge(File folder, String output) throws IOException {
        LinkedList<long[][]> LL = new LinkedList<>();
        File [] files = folder.listFiles();
        DataOutputStream DOStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(output, true)));
        long [][] bucketsAux;
        LinkedList<DataInputStream> DIStreams = new LinkedList<>();
        for(int k =0; k< files.length; k++){
            DIStreams.addLast(new DataInputStream(new BufferedInputStream(new FileInputStream(files[k]))));
        }
        Int2ObjectOpenHashMap<long[]> buffer = new Int2ObjectOpenHashMap<>();
        buffer.defaultReturnValue(null);
        int step = 0;
        long [] buffLong;
        int dumps =0;
        while(DIStreams.size()>1) {
            int section =  50000000/(files.length+2);
            System.out.print("Loading data...");
            partialNow = System.currentTimeMillis();
            for (int i = 0; i < DIStreams.size(); i++ ) {
                System.out.print(" " + LL.size());
                bucketsAux = new long[section][2];
                for (int z = 0; z < section; z++) {
                    try{
                        buffLong = buffer.get(i);
                        if(buffLong == null){
                            buffLong = new long[]{DIStreams.get(i).readLong(),DIStreams.get(i).readLong()};
                        } else buffer.remove(i);
                        if(buffLong[0]<=maxData){
                            bucketsAux[z] = buffLong;
                        } else{
                            buffer.put(i,buffLong);
                            if(z!=0)
                                bucketsAux = Arrays.copyOfRange(bucketsAux,0,z-1);
                            else
                                bucketsAux = null;
                            break;
                        }
                    }catch (EOFException e){
                        if(z!=0) bucketsAux = Arrays.copyOfRange(bucketsAux,0,z-1);
                        else  bucketsAux = null;
                        DIStreams.remove(i);
                        System.out.println(" - List " + i + " Removed -");
                        i--;
                        break;
                    }
                }
                if(bucketsAux!=null){
                    LL.addLast(bucketsAux);
                    if(i == 0){
                        if(bucketsAux[bucketsAux.length-1] != null) maxData = bucketsAux[bucketsAux.length-1][0];
                        section= section*2;
                    }
                }

                //System.out.println("-"+maxData+"-");
            }maxData = Long.MAX_VALUE;
            System.out.println("\t done: " + (System.currentTimeMillis() - partialNow)/1000 + "s");

            System.out.print("Merging data... ");
            partialNow = System.currentTimeMillis();
            for (int i = 0; LL.size() > 2; ) {
                System.out.print(" " + LL.size());
                LL.addLast(mergeSortedMatrix(LL.get(0), LL.get(1), LL.size(), step));
                step++;
                LL.removeFirst();
                LL.removeFirst();
            }
            if(LL.size()==2)
                writeMergeSortedMatrix(LL.get(0), LL.get(1), DOStream);
            else if(LL.size() == 1){
                for (long [] auxArr :LL.get(0)) for (long auxLong : auxArr) DOStream.writeLong(auxLong);
            }
            LL.clear();
            System.out.println("\t done: " + (System.currentTimeMillis() - partialNow)/1000 + "s");
            //if(dumps==3) break;
            //else  dumps++;
            }
        DOStream.close();
    }

    public static void writeMergeSortedMatrix(long[][] a, long[][] b, DataOutputStream DOOStream) throws IOException {
        //for (long[] z : a)  System.out.println(z[0]);
        //System.out.println(a.length+"-"+b.length);
        int i = 0, j = 0;
        while (i < a.length && j < b.length) {
            if (comp.compare(a[i], b[j])<0){
                if(i==0 && j ==0) System.out.print("\t\tFirst Element: " + a[i][0]);
                DOOStream.writeLong(a[i][0]);
                DOOStream.writeLong(a[i++][1]);
            }
            else{
                if(i==0 && j ==0) System.out.print("\t\tFirst Element: " + a[j][0]);
                DOOStream.writeLong(b[j][0]);
                DOOStream.writeLong(b[j++][1]);
            }
        }
        while (i < a.length){
            if(i==a.length-1) System.out.print("\tLast Element:" + a[i][0]);
            //System.out.println(i +" i - length" + a.length);
            DOOStream.writeLong(a[i][0]);
            DOOStream.writeLong(a[i++][1]);
        }
        while (j < b.length) {
            //System.out.println(j +" j - lenght" + b.length);
            if(j==b.length-1) System.out.print("\tLast Element:" + b[j][0]);
            DOOStream.writeLong(b[j][0]);
            DOOStream.writeLong(b[j++][1]);
        }
    }

    static void testMassiveBinaryMerge() throws IOException {
        System.out.println("Testing...");
        DataInputStream DIStream= new DataInputStream(new BufferedInputStream(new FileInputStream("/home/aalto/IdeaProjects/PredictiveIndex/data/dump/sortedInvertedIndex.bin")));
        long  nuovo;
        long vecchio = 0;
        long i;
        for(i =0; true; i++){
            try {
                nuovo = DIStream.readLong();
                if(vecchio-nuovo>0){
                    System.out.println("Difference: "+ (vecchio-nuovo)+"\t Previous:\t"+vecchio);
                    System.out.println("Pointer: "+i+ "\t\t New:\t\t"+nuovo);

                }
                DIStream.readLong();
                vecchio = nuovo;
            }catch (EOFException e){
                break;
            }

        }
        System.out.println("Totale" + i);
    }


    static void testMassiveBinaryMerge2(File folder) throws IOException {
        File [] files = folder.listFiles();
        long  nuovo =0;
        long vecchio = 0;
        DataInputStream DIStream;
        for(int k =0; k< files.length; k++){
            DIStream = new DataInputStream(new BufferedInputStream(new FileInputStream(files[k])));
            System.out.println(files[k].getName() + "-" + k);
            for(int i =0; true; i++){
                //if(i%10000000==0) System.out.println(i);
                try {
                    nuovo = DIStream.readLong();
                    if(vecchio-nuovo>0) System.out.println(vecchio-nuovo+"-"+vecchio+"-"+nuovo);
                    DIStream.readLong();
                    vecchio = nuovo;
                }catch (EOFException e){
                    break;
                }
            }

        }
    }




}

