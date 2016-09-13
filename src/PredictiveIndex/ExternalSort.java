package PredictiveIndex;

import java.io.*;
import java.util.Comparator;

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

    public static void readFiles(File folder, String output) throws IOException {
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
                bucketsA[i] = mergeSortedMatrix(bucketsB[(i*2)], bucketsB[(i*2)+1]);
                bucketsB[(i*2)]   = null;
                bucketsB[(i*2)+1] = null;
            }
        }
        writeMergeSortedMatrix(bucketsA[0], bucketsA[1], DOStream);


    }

    public static long[][] mergeSortedMatrix(long[][] a, long[][] b) {

        long [][] answer = new long[a.length + b.length][2];
        int i = 0, j = 0, k = 0;

        while (i < a.length && j < b.length) {
            if (comp.compare(a[i], b[j])<0)
                answer[k++] = a[i++];
            else
                answer[k++] = b[j++];
        }
        System.arraycopy(a, i, answer, k, (a.length -i));
        System.arraycopy(b, j, answer, k, (b.length -j));
        return answer;
    }

    public static void writeMergeSortedMatrix(long[][] a, long[][] b, DataOutputStream DOOStream) throws IOException {

        int i = 0, j = 0;
        while (i < a.length && j < b.length) {
            if (comp.compare(a[i], b[j])<0){
                DOOStream.writeLong(a[i][0]);
                DOOStream.writeLong(a[i++][1]);
            }
            else{
                DOOStream.writeLong(b[j][0]);
                DOOStream.writeLong(b[j++][1]);
            }
        }
        while (i < a.length){
            DOOStream.writeLong(a[i][0]);
            DOOStream.writeLong(a[i++][1]);
        }
        while (j < b.length) {
            DOOStream.writeLong(b[j][0]);
            DOOStream.writeLong(b[j++][1]);
        }
    }





}

