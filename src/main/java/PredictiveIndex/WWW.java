package PredictiveIndex;

import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
//import org.lemurproject.kstem.KrovetzStemmer;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.System.out;



/**
 * Created by aalto on 10/1/16.
 */
public abstract class WWW {
    /**This interface of the project
     *
     * git diff --stat 4b825dc642cb6eb9a060e54bf8d69288fbee4904 6235 lines
     *
     */


    static final String ROOT        = System.getProperty("user.dir")+"/data/";

    //folders
    static final String [] FOLDER  = new String[]{
            ROOT + 0 +"/",
            ROOT + 1 +"/",
            ROOT + 2 +"/",
            ROOT + 3 +"/"};

    static final String [] CW       = new String[]{FOLDER[0]+"clueweb",FOLDER[1]+"clueweb",FOLDER[2]+"clueweb",FOLDER[3]+"clueweb"};
    static final String [] docStat  = new String[]{FOLDER[0]+"localTermStats",FOLDER[1]+"localTermStats",FOLDER[2]+"localTermStats",FOLDER[3]+"localTermStats"};
    static final String [] docInfo  = new String[]{FOLDER[0]+"docInfo.csv",FOLDER[1]+"docInfo.csv",FOLDER[2]+"docInfo.csv",FOLDER[3]+"docInfo.csv"};

    //////////////////////////////////////////////////////////////////////////////////////////

    static final String SOURCE	    	= ROOT + "SOURCE/";                                                 //F

        static final String MODELS	    	= SOURCE + "MODELS/";                                           //F
            static final String LANGUAGE	    = MODELS + "LANGUAGE/";                                     //F
                static final String UNIGRAMLANGUAGEMODEL	        = LANGUAGE + "UNIGRAMLANGUAGEMODEL";
                static final String UNIGRAMLANGUAGEMODELCONVERTED	= LANGUAGE + "UNIGRAMLANGUAGEMODELCONVERTED";
                static final String UNIGRAMLANGUAGEMODELMAPPING	    = LANGUAGE + "UNIGRAMLANGUAGEMODELMAPPING";
                static final String BIGRAMLANGUAGEMODEL	    	    = LANGUAGE + "BIGRAMLANGUAGEMODEL";
                static final String BIGRAMLANGUAGEMODELCONVERTED    = LANGUAGE + "BIGRAMLANGUAGEMODELCONVERTED";
                static final String BIGRAMLANGUAGEMODELMAPPING	    = LANGUAGE + "BIGRAMLANGUAGEMODELMAPPING";
            static final String QUALITY         = MODELS + "QUALITY/";                                      //F
                static final String UNIGRAMQUALITYMODEL     = QUALITY + "UNIGRAMQUALITYMODEL";
                static final String HITQUALITYMODEL	        = QUALITY + "HITQUALITYMODEL";
                static final String BIGRAMQUALITYMODEL	    = QUALITY + "BIGRAMQUALITYMODEL";
                static final String DBIGRAMQUALITYMODEL 	= QUALITY + "DBIGRAMQUALITYMODEL";
             static final String EMPTY          = MODELS + "EMPTY/";                                        //F
                static final String EMPTYGROUND	            = EMPTY + "EMPTYGROUND";
                static final String EMPTYINDEX	            = EMPTY + "EMPTYINDEX";

        static final String GROUND_TRUTH	= SOURCE + "GROUND_TRUTH/";                                     //F
            static final String COMPLEXRANKERTOP    =   GROUND_TRUTH + "complexRankerResultsTotalNew";

        static final String TRAIN_TEST	    = SOURCE + "TRAIN_TEST/";                                       //F
            static final String TRAIN_TESTQ               = TRAIN_TEST + "million09_all";

        static final String HITDATA         = SOURCE + "HITDATA/";                                          //F
            static final String HITSCORES         = HITDATA + "hitScores";
            static final String HITSCORESCSV      = HITDATA + "hitScores.csv";

        static final String CLUEWEBDATA     = SOURCE + "CLUEWEBDATA/";                                          //F
            static final String CLUEWEB         = CLUEWEBDATA + "noStemmerIndex";
            static final String TERMMAP         = CLUEWEBDATA + "termIDs";
            static final String OLDDOCINFO      = CLUEWEBDATA + "oldDocInfo";
            static final String DOCINFO         = CLUEWEBDATA + "docInfo";
            static final String DIDNAMEMAP      = CLUEWEBDATA + "didNameMap";
            static final String DIDMAP          = CLUEWEBDATA + "didMap";
            static final String DIDMAPSER       = CLUEWEBDATA + "didMapser";
            static final String LOCALTERMFREQ	= CLUEWEBDATA + "LOCALTERMFREQ";
            static final String GLOBALSTATS	    = CLUEWEBDATA + "GLOBALSTATS";

    //////////////////////////////////////////////////////////////////////////////////////////*

    static final String RESULTS	    = ROOT + "TRAIN_RESULTS/";                                 //F
    //static final String RESULTS	        = ROOT + "TEST_RESULTS/";

        static final String METADATA	    	= RESULTS + "METADATA/";                    //F
            static final String LENGTHS	    	    = METADATA + "LENGTHS/";                     //F
                static final String UNILENGTHS      = LENGTHS + "UNILENGTHS";
                static final String HITLENGTHS      = LENGTHS + "HITLENGTHS";
                static final String BILENGTHS       = LENGTHS + "BILENGTHS";
                static final String DBILENGTHS      = LENGTHS + "DBILENGTHS";
                static final String UNIBUCKET       = LENGTHS + "UNIBUCKET";
                static final String HITBUCKET       = LENGTHS + "HITBUCKET";
                static final String BIBUCKET        = LENGTHS + "BIBUCKET";
                static final String DBIBUCKET       = LENGTHS + "DBIBUCKET";
                static final String AGUTERMACCES    = LENGTHS + "AGUTERMACCES";
            static final String FILTER_SETS	    	= METADATA + "FILTER_SET/" ;                                //F
                static final String UNIGRAM_SMALL_FILTER_SET      = FILTER_SETS + "bigramSmallFilterSet";
                static final String BIGRAM_SMALL_FILTER_SET       = FILTER_SETS + "unigramSmallFilterSet";
                static final String BIG_FILTER_SET                = FILTER_SETS + "bigFilterSet";
            static final String ACCESSMAP	    	= METADATA + "ACCESSMAP";

        static final String INDEXES	    	    = RESULTS + "INDEXES/";
            static final String UNIGRAMRAW	    	    = INDEXES + "UNIGRAMRAW/";              //F
            static final String BIGRAMRAW	    	    = INDEXES + "BIGRAMRAW/";               //F
            static final String DBIGRAMRAW	    	    = INDEXES + "DBIGRAMRAW/";              //F
            static final String HITRAW   	            = INDEXES + "HITRAW/";                  //F
            static final String FINAL                   = INDEXES + "FINAL/";                   //F
                static final String UNIGRAMINDEX	    	= FINAL + "UNIGRAMINDEX";
                static final String UNIGRAMINDEX1000  	    = FINAL + "UNIGRAMINDEX1000";
                static final String BIGRAMINDEX	    	    = FINAL + "BIGRAMINDEX";
                static final String DBIGRAMINDEX	    	= FINAL + "DBIGRAMINDEX";
                static final String HITINDEX	    	    = FINAL + "HITINDEX";
                static final String UNIGRAMMETA	        	= FINAL + "UNIGRAMMETA";
                static final String UNIGRAM1000META  	    = FINAL + "UNIGRAM1000META";
                static final String BIGRAMMETA         	    = FINAL + "BIGRAMMETA";
                static final String DBIGRAMMETA	        	= FINAL + "DBIGRAMMETA";
                static final String HITMETA	    	        = FINAL + "HITMETA";

        static final String QUERY	    	= RESULTS + "QUERY/";                                             //F
            static final String Q           = QUERY + "Q";
            static final String QCONVERTED  = QUERY + "Qconverted";
            static final String QBIGRAM     = QUERY + "Qbigram";
            static final String QAGUMENTED  = QUERY + "Qagumented";

    //////////////////////////////////////////////////////////////////////////////////////////

        static final String QUERY_TRACE	        = RESULTS + "QUERY_TRACE/";                        //F
            static final String FILLEDGROUND        = QUERY_TRACE + "FILLEDGROUND";
            static final String FILLEDUNIGRAM       = QUERY_TRACE + "FILLEDUNIGRAM";
            static final String FILLEDBIGRAM        = QUERY_TRACE + "FILLEDBIGRAM";
            static final String FILLEDDBIGRAM       = QUERY_TRACE + "FILLEDDBIGRAM";
            static final String FILLEDHIT           = QUERY_TRACE + "FILLEDHIT";

        static final String SELECTED_CHUNKS	    = RESULTS + "SELECTED_CHUNKS/";
                static final String SELECTED_CHUNKS_UNIGRAM	    = SELECTED_CHUNKS + "SELECTED_CHUNKS_UNIGRAM";
                static final String SELECTED_CHUNKS_HIT	    	= SELECTED_CHUNKS + "SELECTED_CHUNKS_HIT";
                static final String SELECTED_CHUNKS_BIGRAM	    = SELECTED_CHUNKS + "SELECTED_CHUNKS_BIGRAM";
                static final String SELECTED_CHUNKS_DBIGRAM    	= SELECTED_CHUNKS + "SELECTED_CHUNKS_DBIGRAM";

    //////////////////////////////////////////////////////////////////////////////////////////

    
    static Object2IntMap<String> term2IdMap;
    static Int2ObjectOpenHashMap<String>  id2TermMap;
    static double start;
    static double now;
    static double maxBM25 = 0;
    static double minBM25 =2147388309;
    static int totNumDocs = 50220423;

    static final int server = 1;

    //comparators
    static Comparator<int[]> bigramBufferComparator = new Comparator<int[]>() {
        @Override
        public int compare(int[] int1, int[] int2) {
            if (int1[0] == int2[0]) {
                if(int1[1] == int2[1]){
                    return Integer.compare(int2[2], int1[2]);
                }else return Integer.compare(int1[1], int2[1]);
            } else return Integer.compare(int1[0], int2[0]);
        }
    };

    static Comparator<int[]> unigramBufferComparator = new Comparator<int[]>() {
        @Override
        public int compare(int[] int1, int[] int2) {
            if (int1[0] == int2[0]) {
                    return Integer.compare(int2[2], int1[2]);
            } else return Integer.compare(int1[0], int2[0]);
        }
    };

    static Comparator<int[]> singleComparator = new Comparator<int[]>() {
        @Override
        public int compare(int[] int1, int[] int2) {
            if (int1[0] == int2[0]) {
                return Integer.compare(int2[1], int1[1]);
            } else return Integer.compare(int1[0], int2[0]);
        }
    };

    static Comparator<int[]> hitComparator = new Comparator<int[]>() {
        @Override
        public int compare(int[] int1, int[] int2) {
            if (int1[0] == int2[0]) {
                return Integer.compare(int2[2], int1[2]);
            } else return Integer.compare(int1[0], int2[0]);
        }
    };

    static Comparator<int[]> testComparator = new Comparator<int[]>() {
        @Override
        public int compare(int[] int1, int[] int2) {
            if (int1[3] == int2[3]) {
                if(int1[0] == int2[0]){
                    return Integer.compare(int2[1], int1[1]);
                }else return Integer.compare(int1[0], int2[0]);
            } else return Integer.compare(int1[3], int2[3]);
        }
    };

    static Comparator<double[]> rankerComparator = new Comparator<double[]>() {
        @Override
        public int compare(double[] int1, double[] int2) {
            if (int1[0] == int2[0]) {
                return Double.compare(int2[3],int1[3]);
            } else return Double.compare(int1[0], int2[0]);
        }
    };

    static void getTerm2IdMap() throws IOException {
        System.out.println("Fetching Term-TermID map...");
        BufferedReader br = getBuffReader(TERMMAP);
        term2IdMap = new Object2IntOpenHashMap<>();
        String line;
        String [] record;
        int k = 1;
        while ((line = br.readLine()) != null) {
            record = line.split(" ");
            term2IdMap.put(record[1], k++);
        }
        System.out.println("Map fetched!");
    }

    static void getId2TermMap() throws IOException {
        System.out.println("Fetching TermID-Term map...");
        BufferedReader br = getBuffReader(TERMMAP);
        id2TermMap = new Int2ObjectOpenHashMap<>();
        String line;
        String [] record;
        int k = 1;
        while ((line = br.readLine()) != null) {
            record = line.split(" ");
            id2TermMap.put(k++, record[1]);
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

    static long[] getCombinations(List<Integer> superSet, int k, boolean keepOriginal) {
        List<Set<Integer>> res = new ArrayList<>();
        superSet.removeIf(Objects::isNull);
        getSubsets(superSet, k, 0, new HashSet<>(), res);
        long [] combo;
        if(keepOriginal)
            combo = new long[res.size() + superSet.size()];
        else
            combo = new long[res.size()];
        int[] pair;
        int p = 0;
        //System.out.println(res);
        for (Set set : res) {
            pair = Ints.toArray(set);
            java.util.Arrays.parallelSort(pair);
            combo[p] = getPair(pair[0], pair[1]);
            p++;
        }
        if(keepOriginal){
            for(int a : superSet){
                combo[p]=a;
                p++;
            }
        }

        return combo;
    }

    static long[] getBigrams(String [] queryTerms) {
        //this method return all the combination of the docID in the document

        LinkedList<Integer> queryInt = new LinkedList<>();
        int termID;
        String stemmedTerm;
        //KrovetzStemmer stemmer = new KrovetzStemmer();

        // We convert our String [] to int [] using the term-termID map
        for (int i = 0; i < queryTerms.length; i++) {
            try{
                queryInt.add(term2IdMap.get(queryTerms[i]));
            }catch (NullPointerException e){
                System.out.println("Missing: "+queryTerms[i]);
            }
        }
        //We take every combination of our query terms. We save them in a long array using bit-shifting
        return getCombinations(queryInt, 2, true);
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
        return
                new BufferedWriter(new FileWriter(path));
    }

    static ObjectInputStream getOIStream(String filename, boolean buffered) throws IOException {
        if(buffered)
            return new ObjectInputStream(new FileInputStream(filename+".bin"));
        else
            return new ObjectInputStream( new BufferedInputStream(new FileInputStream(filename+".bin")));

    }

    static ObjectOutputStream getOOStream(String filename, boolean buffered) throws IOException {
        if(buffered)
            return new ObjectOutputStream(new FileOutputStream(filename+".bin"));
        else
            return new ObjectOutputStream( new BufferedOutputStream(new FileOutputStream(filename+".bin", false)));
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

    public static List<Integer> string2IntList(String orginal, String sep){
        return Stream.of(orginal.split(sep)).mapToInt(Integer::parseInt).boxed().collect(Collectors.toList());

    }

    public static int[] string2IntArray(String orginal, String sep){
        return Stream.of(orginal.split(sep)).mapToInt(Integer::parseInt).toArray();

    }

    public static List<Long> string2LongList(String orginal, String sep){
        return Stream.of(orginal.split(sep)).mapToLong(Long::parseLong).boxed().collect(Collectors.toList());

    }

    public static long[] string2LongArray(String orginal, String sep){
        return Stream.of(orginal.split(sep)).mapToLong(Long::parseLong).toArray();

    }

    public static List<Double> string2DoubleList(String orginal, String sep){
        return Stream.of(orginal.split(sep)).mapToDouble(Double::parseDouble).boxed().collect(Collectors.toList());

    }

    public static double[] string2DoubleArray(String orginal, String sep){
        return Stream.of(orginal.split(sep)).mapToDouble(Double::parseDouble).toArray();

    }
}

/** Global Statistics
 *
 * Removed Pairs from filter    : 3244860
 * Filter set size:             : 11050140

 */