package PredictiveIndex;

import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.lemurproject.kstem.KrovetzStemmer;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static java.lang.System.out;

/**
 * Created by aalto on 10/1/16.
 */
public abstract class WWW {
    /**This interface of the project
     *
     */

    //root
    static final String root        = "/home/aalto/IdeaProjects/PredictiveIndex/data/";

    //folders
    static final String [] folder  = new String[]{root + 0 +"/", root + 1 +"/", root + 2 +"/", root + 3 +"/"};

    static final String metadata    = root + "metadata/";
    static final String results     = root + "results/";
    static final String models      = root + "models/";
    static final String source      = root + "source/";
    static final String maps         = root + "maps/";

    //threads
    static final String [] CW       = new String[]{folder[0]+"clueweb",folder[1]+"clueweb",folder[2]+"clueweb",folder[3]+"clueweb"};
    static final String [] docStat  = new String[]{folder[0]+"localTermStats",folder[1]+"localTermStats",folder[2]+"localTermStats",folder[3]+"localTermStats"};
    static final String [] docInfo  = new String[]{folder[0]+"docInfo.csv",folder[1]+"docInfo.csv",folder[2]+"docInfo.csv",folder[3]+"docInfo.csv"};

    //source
    static final String lanModel    = source + "lanModel";
    static final String complexRank = source + "complexRankerResultsTraining";
    static final String trainQ      = source + "trainQ";
    static final String tMap        = source + "termIDs";
    static final String oldDocInfo  = source + "oldDocInfo";
    static final String didNameMap  = source + "didNameMap";
    static final String finalDocInfo= source + "docInfo";

    //metadata
    static final String freqMap     = metadata + "freqMap";
    static final String gStats      = metadata + "globalStats";
    static final String filterSet   = metadata + "filterSet";
    static final String fastQT      = metadata + "fastQT";
    static final String toPick      = metadata + "toPick";

    //maps
    static final String dumpMap     = maps + "dumpMap";
    static final String accessMap   = maps + "accessMap";

    //results
    static final String sortedI2    = results + "sortedI2" ;
    static final String rawI2       = results + "rawI2/";
    static final String pListLength = results + "PListLength";
    static final String pairProbLen = results + "PairProbLen";
    static final String selected    = results + "selected";

    //models
    static final String partialModel= models + "partialModel";
    static final String qualityModel= models + "qualityModel";
    static final String sortedRange = models + "sortedRanges";


    static Object2IntMap<String> termMap;
    static double start;
    static double now;
    static double maxBM25 = 0;
    static double minBM25 =2147388309;
    static int totNumDocs = 50220423;


    static void getTermMap() throws IOException {
        System.out.println("Fetching Term-TermID map...");
        BufferedReader br = getBuffReader(tMap);
        termMap = new Object2IntOpenHashMap<>();
        String line;
        String [] record;
        int k = 1;
        while ((line = br.readLine()) != null) {
            record = line.split(" ");
            termMap.put(record[1], k++);
        }
        System.out.println("Map fetched!");
    }


    private static void getSubsets(List<Integer> superSet, int k, int idx, Set<Integer> current, List<Set<Integer>> solution) {
        //successful stop clause
        if (current.size() == k) {
            solution.add(new HashSet<>(current));
            return;
        }
        //unseccessful stop clause
        if (idx == superSet.size()) return;
        Integer x = superSet.get(idx);
        current.add(x);
        //"guess" x is in the subset
        getSubsets(superSet, k, idx + 1, current, solution);
        current.remove(x);
        //"guess" x is not in the subset
        getSubsets(superSet, k, idx + 1, current, solution);
    }

    static long[] getCombinations(List<Integer> superSet, int k) {
        List<Set<Integer>> res = new ArrayList<>();
        superSet.removeIf(Objects::isNull);
        getSubsets(superSet, k, 0, new HashSet<>(), res);
        long[] combo = new long[res.size()];
        int[] pair;
        int p = 0;
        //System.out.println(res);
        for (Set set : res) {
            pair = Ints.toArray(set);
            java.util.Arrays.parallelSort(pair);
            combo[p] = getPair(pair[0], pair[1]);
            p++;
        }
        return combo;
    }

    static long[] getBigrams(String[] queryTerms) {
        //this method return all the combination of the docID in the document

        LinkedList<Integer> queryInt = new LinkedList<>();
        int termID;
        String stemmedTerm;
        KrovetzStemmer stemmer = new KrovetzStemmer();

        // We convert our String [] to int [] using the term-termID map
        for (int i = 0; i < queryTerms.length; i++) {
            try{
                queryInt.add(termMap.get(queryTerms[i]));
            }catch (NullPointerException e){
                System.out.println(queryTerms[i]);
            }
        }
        //We take every combination of our query terms. We save them in a long array using bit-shifting
        return getCombinations(queryInt, 2);
    }

    static boolean checkExistence(String path){
        return Files.exists(Paths.get(path)) | Files.exists(Paths.get(path+".bin"));
    }

    static DataInputStream getDIStream(String path) throws FileNotFoundException {
        return new DataInputStream(new BufferedInputStream(new FileInputStream(path+".bin")));
    }

    static DataOutputStream getDOStream(String path) throws FileNotFoundException {
        return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(path+".bin", false)));
    }

    static BufferedReader getBuffReader(String path) throws FileNotFoundException {
        return  new BufferedReader(new FileReader(path));
    }

    static BufferedWriter getBuffWriter(String path) throws IOException {
        return  new BufferedWriter(new FileWriter(path));
    }

    static ObjectInputStream getOIStream(String filename, boolean buffered) throws IOException {
        if(buffered) return new ObjectInputStream(new FileInputStream(filename+".bin"));
        else return new ObjectInputStream( new BufferedInputStream(new FileInputStream(filename+".bin")));

    }

    static ObjectOutputStream getOOStream(String filename, boolean buffered) throws IOException {
        if(buffered) return new ObjectOutputStream(new FileOutputStream(filename+".bin"));
        else return new ObjectOutputStream( new BufferedOutputStream(new FileOutputStream(filename+".bin", false)));
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
        out.print("Fetching: " + file + "...");
        Object e = null;
        try {
            ObjectInputStream OIStream = getOIStream(file, true);
            e = OIStream.readObject();
            OIStream.close();
            out.println("\tfetched!");
            return e;
        } catch (IOException i) {
            i.printStackTrace();
            return null;
        } catch (ClassNotFoundException c) {
            out.println("Object not found");
            c.printStackTrace();
            return null;
        }
    }

    static long getPair(int a , int b){
        return (long)a << 32 | b & 0xFFFFFFFFL;
    }

    static int[] getTerms(long c){
        int aBack = (int)(c >> 32);
        int bBack = (int)c;
        return new int[]{aBack,bBack};
    }
}

/** Global Statistics
 *
 * Removed Pairs from filter    : 3244860
 * Filter set size:             : 11050140

 */