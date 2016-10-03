package PredictiveIndex;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.doubles.Double2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.doubles.Double2FloatOpenHashMap;
import it.unimi.dsi.fastutil.doubles.Double2LongRBTreeMap;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.lemurproject.kstem.KrovetzStemmer;

import static PredictiveIndex.ExternalSort.massiveBinaryMerge;
import static PredictiveIndex.ExternalSort.sortSmallInvertedIndex;
import static PredictiveIndex.InvertedIndex.*;


import static PredictiveIndex.utilsClass.*;



/**
 * Created by aalto on 7/20/16.
 */
public class PredictiveIndex {
    static long counter = 1;
    static int counter2 = 1;
    private static String metadata = "/home/aalto/IdeaProjects/PredictiveIndex/data/metadata/";
    private static String qi = "/home/aalto/dio/";
    static final String clueweb09 = "/home/aalto/IdeaProjects/PredictiveIndex/data/clueweb/";
    static final String dataFold = "/home/aalto/IdeaProjects/PredictiveIndex/data/";
    static final String globalFold = dataFold + "global/";







    /*******************************************************************************************************************/

    /*******************************************************************************************************************/











}





