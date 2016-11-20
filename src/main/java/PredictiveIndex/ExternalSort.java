package PredictiveIndex;

import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.io.*;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.ListIterator;

import static PredictiveIndex.WWW.dBigramIndex;
import static PredictiveIndex.WWW.getBuffWriter;
import static PredictiveIndex.WWW.server;
import static PredictiveIndex.utilsClass.getTerms;

/**
 * Created by aalto on 9/12/16.
 */
public class ExternalSort {
    private static String metadata = "/home/aalto/IdeaProjects/PredictiveIndex/data/metadata/";
    private static String qi = "/home/aalto/dio/";
    static final String clueweb09 = "/home/aalto/IdeaProjects/PredictiveIndex/data/clueweb/";
    static final String dataFold = "/home/aalto/IdeaProjects/PredictiveIndex/data/";
    static final String globalFold = dataFold+"global/";
    static long [][] bucket;
    private static long now = System.currentTimeMillis();
    static long partialNow;

    static BufferedWriter bw;
    static int linesCount = 0;
    static boolean fourFields = false;

    static Comparator<long[]> comp = new Comparator<long[]>() {
        @Override
        public int compare(long[] int1, long[] int2) {
            //if we have the same doc ids sort them based on the bm25
            if (int1[0] == int2[0]) {
                return Long.compare(int2[1], int1[1]);
            } else return Long.compare(int1[0], int2[0]);
        }
    };

    static long pairMax = Long.MAX_VALUE;
    static long BM25max = Long.MAX_VALUE;



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



    public static void massiveBinaryMerge(File folder, String output, boolean fFields) throws IOException {
        bw = getBuffWriter(dBigramIndex+"index.csv");
        fourFields = fFields;

        LinkedList<long[][]> LL = new LinkedList<>();
        File [] files = folder.listFiles();
        DataOutputStream DOStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(output+".bin", false)));
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
            int section =  (100000000*server)/(files.length+2);
            System.out.print("Loading data...");
            partialNow = System.currentTimeMillis();
            for (int i = 0; i < DIStreams.size(); i++) {
                if(LL.size()%50 !=0 & LL.size()!=0){
                    System.out.print(" " + LL.size());
                }else{
                    System.out.println(" "+LL.size());
                }
                bucketsAux = new long[section][2];
                for (int z = 0; z < section; z++){
                    try{
                        buffLong = buffer.get(i);
                        if(buffLong == null){
                            if(fourFields)
                                buffLong = new long[]{DIStreams.get(i).readLong(),DIStreams.get(i).readLong()};
                            else
                                buffLong = new long[]{DIStreams.get(i).readLong(),DIStreams.get(i).readInt()};
                            //linesCount++; //sometimes reading from the stream fails
                        } else buffer.remove(i);
                        if(buffLong[0]<pairMax){
                            bucketsAux[z] = buffLong;
                            linesCount++;
                        }else if (buffLong[0] == pairMax & buffLong[0] <= BM25max){
                            bucketsAux[z] = buffLong;
                        }else {
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
                        if(bucketsAux[bucketsAux.length-1] != null){
                            pairMax = bucketsAux[bucketsAux.length-1][0];
                            BM25max = bucketsAux[bucketsAux.length-1][1];
                        }
                        section= section*4;
                        System.out.println(Arrays.toString(getTerms(pairMax)));
                    }
                }

                //System.out.println(Arrays.toString(getTerms(maxData)));
            }pairMax = Long.MAX_VALUE;
            BM25max = Long.MAX_VALUE;
            System.out.print("\t done: " + (System.currentTimeMillis() - partialNow)/1000 + "s");

            System.out.print("Merging data... ");
            partialNow = System.currentTimeMillis();
            for (int i = 0; LL.size() > 2; ) {
                if(LL.size()%50 !=0 & LL.size()!=386){
                    System.out.print(" " + LL.size());
                }else{
                    System.out.println(" "+LL.size());
                }
                LL.addLast(mergeSortedMatrix(LL.get(0), LL.get(1), LL.size(), step));
                step++;
                LL.removeFirst();
                LL.removeFirst();
            }
            if(LL.size()==2) {
                writeMergeSortedMatrix(LL.get(0), LL.get(1), DOStream);
                LL.removeFirst();
                LL.removeFirst();
            }
            else if(LL.size() == 1){
                for (long [] auxArr :LL.get(0)) for (long auxLong : auxArr) DOStream.writeLong(auxLong);
            }
            LL.clear();
            System.gc();
            System.out.println("\t done: " + (System.currentTimeMillis() - partialNow)/1000 + "s");
            //if(dumps==3) break;
            //else  dumps++;
            }
        System.out.println(linesCount);
        bw.close();
        DOStream.close();
    }

    private static void binaryEntryWrite(DataOutputStream DOStream, long [] a) throws IOException {
        if(fourFields){
            DOStream.writeLong(a[0]);
            DOStream.writeLong(a[1]);
        }else{
            DOStream.writeLong(a[0]);
            DOStream.writeInt((int) a[1]);
        }
    }

    private static void UTF8EntryWrite(DataOutputStream DOStream, long [] a) throws IOException {
        if(fourFields){
            bw.write(Arrays.toString(getTerms(a[0])));
            bw.write(Arrays.toString(getTerms(a[1])));
            bw.newLine();
        }else {
            bw.write(Arrays.toString(getTerms(a[0])));
            bw.write((int) a[1]);
            bw.newLine();
        }
    }

    public static void writeMergeSortedMatrix(long[][] a, long[][] b, DataOutputStream DOStream) throws IOException {
        //for (long[] z : a)  System.out.println(z[0]);
        //System.out.println(a.length+"-"+b.length);
        int i = 0, j = 0;
        while (i < a.length && j < b.length) {
            if (comp.compare(a[i], b[j])<0){
                if(i==0 && j ==0) System.out.print("\t\tFirst Element: " + Arrays.toString(getTerms(a[i][0])));
                binaryEntryWrite(DOStream, a[i++]);
            }
            else{
                if(i==0 && j ==0) System.out.print("\t\tFirst Element: " + Arrays.toString(getTerms(b[j][0])));
                binaryEntryWrite(DOStream, b[j++]);
            }
        }
        while (i < a.length){
            if(i==a.length-1) System.out.print("\tLast Element:" +  Arrays.toString(getTerms(a[i][0])));
            //System.out.println(i +" i - length" + a.length);
            binaryEntryWrite(DOStream, a[i++]);
        }
        while (j < b.length) {
            //System.out.println(j +" j - lenght" + b.length);
            if(j==b.length-1) System.out.print("\tLast Element:" +  Arrays.toString(getTerms(b[j][0])));
            binaryEntryWrite(DOStream, b[j++]);
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
            //System.out.println(files[k].getName() + "-" + k);
            for(int i =0; true; i++){
                //if(i%10000000==0) System.out.println(i);
                try {
                    nuovo = DIStream.readLong();
                    if(vecchio-nuovo>0) System.out.println(k+"---"+(vecchio-nuovo)+"-"+Arrays.toString(getTerms(vecchio))+"-"+Arrays.toString(getTerms(nuovo)));
                    DIStream.readLong();
                    vecchio = nuovo;
                }catch (EOFException e){
                    break;
                }
            }

        }
    }



    static void sortSmallInvertedIndex() throws IOException {
        long [][] bucketsAux;
        DataInputStream DIS;
        long [][] i2= new long[280000000][2];
        int i=0;
        File folder = new File(globalFold + "rawInvertedIndex/");
        File [] files = folder.listFiles();

        for (File f:files) {
            DIS = new DataInputStream(new BufferedInputStream(new FileInputStream(f)));
            for (; i < i2.length; i++) {
                try{
                    i2[i][0] = DIS.readLong();
                    i2[i][1] = DIS.readLong();
                }catch (EOFException e){
                    System.out.println(f.getName());
                    break;
                }
            }
        }
        Arrays.parallelSort(i2,0,i,comp);
        DataOutputStream DOS = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(globalFold+"invertedIndex.bin", true)));
        for (long[] x : i2){
            DOS.writeLong(x[0]);
            DOS.writeLong(x[1]);
        }
        DOS.close();
    }









}

