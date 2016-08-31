package PredictiveIndex;

/**
 * Created by aalto on 7/8/16.
 */


import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

public class ImpactModel {

    public static void buildImpactModel() throws IOException {
        LinkedList<Integer[]> postings = new LinkedList<>();
        Scanner scanner = new Scanner(new File("/home/aalto/IdeaProjects/PredictiveIndex/dumps/finale.txt"), "UTF-8");
        scanner.useDelimiter(",");
        Integer[] n = new Integer[2];
        String[] now;
        String[] p = {"ffameoimcio","nfeafnuane"};
        Integer[][] aux;
        int counter;
        PrintWriter out = createDumpFile();
        while (scanner.hasNextLine()) {
            now = scanner.nextLine().split(",");
            if(now[0].equals(p[0]) & now[1].equals(p[1])){
                n[0] = Integer.valueOf(now[2]); //rank
                n[1] = Integer.valueOf(now[3]); //docID
                postings.add(n);
            } else {
                System.out.println(now[0]);
                aux = new Integer[postings.size()][2];
                Iterator<Integer[]> it = postings.iterator();
                counter = 0;
                while (it.hasNext()) {
                    aux[counter] = it.next();
                    counter++;
                }
                writeOn(sortMerged(aux), out);
                p[0] = now[0];
                p[1] = now[1];
            }
        }
    }

    public static PrintWriter createDumpFile() {
        try (FileWriter fw = new FileWriter("huge.txt", true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            return out;
        } catch (IOException e) {
            System.out.println("No dump, No party!");
            System.exit(1);
            return null;
        }
    }


    public static Integer[][] sortMerged(Integer[][] toSort) {
        Arrays.sort(toSort, new Comparator<Integer[]>() {
            @Override
            public int compare(Integer[] int1, Integer[] int2) {
                //if we have the same doc ids sort them based on the bm25
                return Integer.compare(int1[0], int2[0]);
            }
        });
        return toSort;
    }

    public static void writeOn(Integer[][] toFlush, PrintWriter out) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n"+toFlush.length+",");
        for (Integer aux[] : toFlush) {
            out.print(","+aux[0]);
        }
    }

    public static void main(String [] args) throws IOException {
        buildImpactModel();
    }

}


