package approach4.temporal.skipList;


import approach4.*;
import approach4.MPTPointerBased.Serializer;
import approach4.MPTPointerBased.Trie;
import approach4.temporal.*;
import approach4.temporal.IndexMVWrapper.*;
import approach4.temporal.VersionToKey.IVersionsToKeysIndex;
import approach4.temporal.VersionToKey.VersionsToConsecutiveKeysIndex;
import approach4.temporal.VersionToKey.VersionsToKeysIndex;
import approach4.typeUtils.*;
import approach4.valueDataStructures.TableRowIntDateCols;
import approach4.valueDataStructures.TableRowStrIntCols;
import approach4.valueDataStructures.TableRowStrLongCols;
import org.apache.directory.mavibot.btree.*;
import org.apache.directory.mavibot.btree.exception.KeyNotFoundException;
import org.apache.directory.mavibot.btree.serializer.LongSerializer;
import org.apache.directory.mavibot.btree.serializer.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

//import static org.junit.Assert.assertTrue;

import org.python.util.PythonInterpreter;


public class Main {



    static double iterationProbability = 0.5;
    static final String runsAvgProcessed = "RunsAvgProcessed";
    static int partitionCapacity = 5;

    static final String merkleBucketTree = "merkleBucketTree";
    static final String patriciaMerkleTrie = "patriciaMerkleTrie";
    static final String temporalSkipList = "temporalSkipList";

    // Q: What are these? 
    static final String temporalIndex = "temporalIndex";
    static final String verToIndex = "verToIndex";
    static final String phTreeIndex = "PHTree";
    static final String merkleKDTreeIndex = "MerkleKDTree";

    public static void main(String[] args) throws Exception {

//        btree1();
//        RecordManager recordManager = new RecordManager();
//        BTree<Long, String> btree = recordManager.addBTree( "test", LongSerializer.INSTANCE, StringSerializer.INSTANCE, false );

//        InMemoryTransactionManager recordManager = new InMemoryTransactionManager();

//        btree.close();
//        recordManager.close();





//        Set<Long> expected = new HashSet<Long>();
//        List<Long> added = new ArrayList<Long>();
//
//        Random random = new Random( System.nanoTime() );
//
//
//        int nbElems = 1000;
//
//        BTree<Long, String> btree = BTreeFactory.createInMemoryBTree( "test", LongSerializer.INSTANCE,
//                StringSerializer.INSTANCE );
//        btree.setPageSize( 32 );
//
//        btree.insert(3L, "1");
//        btree.insert(5L, "1");
//        btree.insert(2L, "1");
//        btree.insert(4L, "1");
//        btree.insert(1L, "1");
//
//        btree.
//
//
//        int x = (int) btree.getRevision();
//
//
//        TupleCursor<Long, String> cursor = btree.browse( 5L );
//
//        while ( cursor.hasNext() )
//        {
//            Tuple<Long, String> res = cursor.next();
//            long key = res.getKey();
//
//        }
//        cursor.close();








//        PHTreeP<Integer> phTree = PHTreeP.createPHTree(2);
//        phTree.insert(new double[]{1,1}, 1);
//        phTree.insert(new double[]{1,2}, 6);
//        phTree.insert(new double[]{1,3}, 3);
//        phTree.insert(new double[]{1,4}, 5);
//        phTree.insert(new double[]{1,5}, 2);
//        phTree.insert(new double[]{1,6}, 4);
//
//        System.out.println(phTree.getDims());
//
//        Integer a = phTree.queryExact(new double[]{1,2});
//
//        QueryIterator<PointEntry<Integer>> it = phTree.query(new double[]{1,1}, new double[]{1,10});
//
//        while (it.hasNext()) {
//            PointEntry<Integer> next = it.next();
//
//            System.out.println(next.value());
//
//        }



//        long time = System.currentTimeMillis();
//        //long time = 1645200344189L;
//        System.out.println("seed: " + time);
//        ToweredSkipList.random.setSeed(time);
//        ToweredSkipList.MAX_LEVEL = 0;
//        debugInfo = true;

//        int valuesCount = 10000;
//        int capacity = 10000;
//        ToweredSkipList.MAX_LEVEL = capacity;

//        testTableTenColsIndex();

        String configFilePath = "./config.properties";
        FileInputStream propsInput = new FileInputStream(configFilePath);
        Properties prop = new Properties();
        prop.load(propsInput);

        String logPrefix = prop.getProperty("logPrefix");
        String logPostfix = prop.getProperty("logPostfix");
        Files.createDirectories(Paths.get(logPrefix));
        partitionCapacity = Integer.parseInt(prop.getProperty("partitionCapacity"));


        int dataStart = Integer.parseInt(prop.getProperty("collectEvaluationResultsDataStart"));
        int dataEnd = Integer.parseInt(prop.getProperty("collectEvaluationResultsDataEnd"));
        int dataInc = Integer.parseInt(prop.getProperty("collectEvaluationResultsDataInc"));
        int runs = Integer.parseInt(prop.getProperty("collectEvaluationResultsRuns"));
//        collectEvaluationResultsScenario(logPrefix, logPostfix, dataStart, dataEnd, dataInc, runs, true);


        String itemsCountsStr = prop.getProperty("collectEvaluationResults2ItemsCounts");
        ArrayList<Integer> itemsCounts = Arrays.stream(itemsCountsStr.split(",")).map(Integer::parseInt).collect(Collectors.toCollection(ArrayList::new));
        String versionsCountsStr = prop.getProperty("collectEvaluationResults2versionsCounts");
        ArrayList<Integer> versionsCounts = Arrays.stream(versionsCountsStr.split(",")).map(Integer::parseInt).collect(Collectors.toCollection(ArrayList::new));
        String keysPercentsCountsStr = prop.getProperty("collectEvaluationResults2keysPercentsCounts");
        ArrayList<Double> keysPercentsCounts = Arrays.stream(keysPercentsCountsStr.split(",")).map(Double::parseDouble).collect(Collectors.toCollection(ArrayList::new));
        runs = Integer.parseInt(prop.getProperty("collectEvaluationResults2Runs"));

        // Starts off everything
        collectEvaluationResultsScenario2(logPrefix, logPostfix, itemsCounts, versionsCounts, runs, keysPercentsCounts);

        String collectEvaluationResults3InputCsvFilePath = prop.getProperty("collectEvaluationResults3InputCsvFilePath");
        double collectEvaluationResults3keysPercentsCounts = Double.parseDouble(prop.getProperty("collectEvaluationResults3keysPercentsCounts"));
        CsvReader csvReader = new CsvReader(collectEvaluationResults3InputCsvFilePath);
//        collectEvaluationResultsScenario33(logPrefix, logPostfix, collectEvaluationResults3keysPercentsCounts, csvReader);


//        String collectEvaluationResults3InputCsvFilePath = prop.getProperty("collectEvaluationResults3InputCsvFilePath");
//        double collectEvaluationResults3keysPercentsCounts = Double.parseDouble(prop.getProperty("collectEvaluationResults3keysPercentsCounts"));
//        int collectEvaluationResults3Runs = Integer.parseInt(prop.getProperty("collectEvaluationResults3Runs"));
//        CsvReader csvReader = new CsvReader(collectEvaluationResults3InputCsvFilePath);
//        collectEvaluationResultsScenario3(logPrefix, logPostfix, collectEvaluationResults3Runs, collectEvaluationResults3keysPercentsCounts, csvReader);


//        PythonInterpreter interpreter = new PythonInterpreter();
//        System.out.println("Java runs python code using jython");
//        interpreter.execfile("merkle_kdtree.py");
//        System.out.println("x: " + interpreter.get("x"));
//        System.out.println("x: " + interpreter.get("y"));

    }

    /*
       As the B-tree is browsed, each key is compared with the previously encountered key (elem) to ensure it is strictly greater, 
       adhering to the expected sorted order of keys in a B-tree. This process validates the integrity and correctness of the B-tree's sorting mechanism.
    */ 
    private static boolean btree1() throws IOException, KeyNotFoundException {
        BTree<Long, String> btree = BTreeFactory.createInMemoryBTree( "test", LongSerializer.INSTANCE, StringSerializer.INSTANCE );
        btree.setPageSize(32);


        Random random = new Random( System.nanoTime() );

        int nbElems = 500000;


        // Create a BTree with 500 000 entries
        for ( int i = 0; i < nbElems; i++ )
        {
            long key = ( long ) random.nextLong();
            String value = Long.toString( key );

            try
            {
                btree.insert( key, value );

                if ( i % 100000 == 0 )
                {
                    System.out.println( "Written " + i + " elements" );
                }
            }
            catch ( Exception e )
            {
                e.printStackTrace();
                System.out.println( btree );
                System.out.println( "Error while adding " + value );
                return true;
            }
        }


        ArrayList<Long> elems = new ArrayList<>();
        TupleCursor<Long, String> cursor = btree.browse();

//        int size = 0;
        long elem = Long.MIN_VALUE;

        while ( cursor.hasNext() )
        {
            Tuple<Long, String> res = cursor.next();

            if ( res.getKey() > elem )
            {
                elem = res.getKey();
                elems.add(elem);
//                size++;
            }
        }

        for ( int i = 0; i < elems.size(); i++ )
        {
            try
            {
                String s = btree.get( elems.get(i) );
                System.out.println("got " + s);
            }
            catch ( KeyNotFoundException knfe )
            {
                System.out.println( "Bad tree, missing " + elems.get(i) + ", " + btree );
            }
        }


        btree.close();
        return false;
    }

    /*
     Add two elements to the HashMap filesMetaData
     1. "header" -> header
     2. name -> fileMetaData
     */
    private static void addEntry(String name, String header, HashMap<String, HashMap<String, Object>> filesMetaData) {
        HashMap<String, Object> fileMetaData = new HashMap<>();
        fileMetaData.put("header", header);
        filesMetaData.put(name, fileMetaData);
    }

    /*
     Overall, this function performs data aggregation and averaging based on specified key fields from a source and outputs the results to a destination file (sorted manner)
     srcPath: The file system path to the source file containing the data to be processed
     destPath: The file system path where the aggregated and processed data will be written
     aggKey: A set of integers representing the indexes of the columns to be used as keys for aggregation.
     aggLines: A HashMap where the keys are unique combinations of specified key fields, concatenated with a "|", 
               and the value is a list of string arrays representing lines of data associated with that key.
     lines: An ArrayList containing strings of aggregated and averaged data, ready to be sorted and written to the destination file.
     */
    public static void ProcessRunsAvg(Path srcPath, Path destPath, HashMap<String, Object> destMetaData, Set<Integer> aggKey) throws IOException {

        String header = (String) destMetaData.get("header");
        String[] headerSplit = header.split(",");
        int colsCount = headerSplit.length;

        Files.deleteIfExists(destPath);
        Utils.writeToFile(destPath, destMetaData.get("header") + System.lineSeparator(), StandardOpenOption.CREATE);

        Scanner scanner = new Scanner(srcPath);
        if (scanner.hasNextLine()) {
            String line = scanner.nextLine();
        }

        HashMap<String,ArrayList<String[]>> aggLines = new HashMap<>();
        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            String[] lineSplit = line.split(",");

            ArrayList<String> keyFields = new ArrayList<>();
            for (int i=0; i<colsCount; i++) {
                if (aggKey.contains(i)) {
                    keyFields.add(lineSplit[i]);
                }
            }
            String concatKeyFields = String.join("|",keyFields);
            ArrayList<String[]> curAggLines = aggLines.computeIfAbsent(concatKeyFields, k -> new ArrayList<>());
            curAggLines.add(lineSplit);
        }

        ArrayList<String> lines = new ArrayList<>();
        for (String key : aggLines.keySet()) {
            ArrayList<String[]> lineSplits = aggLines.get(key);
//            lineSplits.remove(0);

            ArrayList<Object> aggLine = new ArrayList<>();
            String[] firstLineSplit = lineSplits.get(0);
            for (int i=0; i<colsCount; i++) {
                if (aggKey.contains(i)) {
                    aggLine.add(firstLineSplit[i]);
                } else {
                    aggLine.add(0.0);
                }
            }


            for (String[] lineSplit : lineSplits) {
                for (int i=0; i<colsCount; i++) {
                    if (!aggKey.contains(i)) {
                        double val = Double.parseDouble(lineSplit[i]);
                        aggLine.set(i, val + (double) aggLine.get(i));
                    }
                }
            }

            long linesCount = lineSplits.size();
            for (int i=0; i<colsCount; i++) {
                if (!aggKey.contains(i)) {
                    aggLine.set(i, (double) aggLine.get(i) /  linesCount);
                }
            }

            List<String> aggLineStr = aggLine.stream().map(Object::toString).collect(Collectors.toList());
            String line = String.join(",", aggLineStr);
            lines.add(line);

        }

        Collections.sort(lines);
        for (String line : lines) {
            Utils.writeToFile(destPath, line + System.lineSeparator(), StandardOpenOption.APPEND);
        }
    }

    /*
     This function takes a Path object pathPatriciaMerkleTrieScenario and a Map<String, Object> patriciaMerkleTrieRunRes
     formats and appends the results of a Patricia Merkle Trie run to the Path. 
     It constructs a CSV string from the map's values, representing different metrics such as count, versions, keysPercent, insert times, etc., to the file.
     */
    private static void writeScenarioResultsToFile(Path pathPatriciaMerkleTrieScenario, Map<String, Object> patriciaMerkleTrieRunRes) {
        String line;
        line = String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                patriciaMerkleTrieRunRes.get("count"), patriciaMerkleTrieRunRes.get("versions"), patriciaMerkleTrieRunRes.get("keysPercent"),
                patriciaMerkleTrieRunRes.get("insertLs"), patriciaMerkleTrieRunRes.get("insertTPs"),
                patriciaMerkleTrieRunRes.get("rangeSearch1SkipListLns"), patriciaMerkleTrieRunRes.get("rangeSearch1MerkleLns"),
                patriciaMerkleTrieRunRes.get("rangeSearch3SkipListLms"), patriciaMerkleTrieRunRes.get("rangeSearch3MerkleLms"),
                patriciaMerkleTrieRunRes.get("rangeSearch4SkipListLms"), patriciaMerkleTrieRunRes.get("rangeSearch4MerkleLms"));
        Utils.writeToFile(pathPatriciaMerkleTrieScenario, line + System.lineSeparator(), StandardOpenOption.APPEND);
    }

    /*
    This function initializes a file at a given Path path by deleting it if it exists and then creating a new file with a header specified in filesMetaData under an entryName.
     */
    private static void initFile(String entryName, HashMap<String, HashMap<String, Object>> filesMetaData, Path path) throws IOException {
        Files.deleteIfExists(path);
        Utils.writeToFile(path, filesMetaData.get(entryName).get("header") + System.lineSeparator(), StandardOpenOption.CREATE);
    }

    /*
     This function generates a list of versioned data from a list of strings strArr and a specified number of versions versionsCount.
     Each string in strArr is converted into a TableRowStrIntCols object <string columnNo, integer columnNo, ITypeUtils for type operations> 
     All these rows are duplicated across the number of versions specified.
     */
    private static ArrayList<ArrayList<TableRowStrIntCols>> generateVersions(List<String> strArr, int versionsCount) {
        ITypeUtils<TableRowStrIntCols> tableRowStrIntColsClassUtils = new TableRowStrIntColsClassUtils();
        ArrayList<ArrayList<TableRowStrIntCols>> versions = new ArrayList<>();
        ArrayList<TableRowStrIntCols> version = new ArrayList<>();

        for (int rowNum = 0; rowNum < strArr.size(); rowNum++) {
            String val = strArr.get(rowNum);
            TableRowStrIntCols tr = new TableRowStrIntCols(val, Integer.parseInt(val), tableRowStrIntColsClassUtils);
            version.add(tr);
        }

        for (int versionNum = 0; versionNum < versionsCount; versionNum++) {
            versions.add(version);
        }

        return versions;
    }

    private static ArrayList<HashMap<String, Object>> getSettings(int dataStart, int dataEnd, int dataInc, int runs) {

        ArrayList<HashMap<String, Object>> settings = new ArrayList<>();
        for (int count = dataStart; count <= dataEnd; count += dataInc) {

            ArrayList<Integer> merkleCapacities = new ArrayList<>();
//            for (int i=0;i<runs;i++) {
//                merkleCapacities.add(100);
//            }
//            for (int i=0;i<runs;i++) {
//                merkleCapacities.add(1000);
//            }
            for (int i=0;i<runs;i++) {
                merkleCapacities.add(10000);
            }
//            for (int i=0;i<runs;i++) {
//                merkleCapacities.add(100000);
//            }
//            for (int i=0;i<runs;i++) {
//                merkleCapacities.add(1000000);
//            }
//            for (int i=0;i<runs;i++) {
//                merkleCapacities.add(10000000);
//            }
//            for (int i=0;i<runs;i++) {
//                merkleCapacities.add(100000000);
//            }

            ArrayList<Integer> temporalIndexCapacities = new ArrayList<>();
//            for (int i=0;i<runs;i++) {
//                temporalIndexCapacities.add(10);
//            }
            for (int i=0;i<runs;i++) {
                temporalIndexCapacities.add(20);
            }
//            for (int i=0;i<runs;i++) {
//                temporalIndexCapacities.add(30);
//            }
//            for (int i=0;i<runs;i++) {
//                temporalIndexCapacities.add(40);
//            }
//            for (int i=0;i<runs;i++) {
//                temporalIndexCapacities.add(80);
//            }
//
//            for (int i=0;i<runs;i++) {
//                temporalIndexCapacities.add(50);
//            }

            HashMap<String, Object> hm = new HashMap<>();
            hm.put("count", count);
            hm.put("merkleCapacities", merkleCapacities);
            hm.put("temporalIndexCapacities", temporalIndexCapacities);
            settings.add(hm);
        }
        return settings;
    }

    public static ArrayList<Integer> generateUniqueRandomNumbers(int init, int step, int count) {
        ArrayList<Integer> list = generateSortedNumbers(init, step, count);
        Collections.shuffle(list);
        return list;
    }

    private static ArrayList<Integer> generateSortedNumbers(int init, int step, int count) {
        ArrayList<Integer> list = new ArrayList<>();
        int cur = init;
        for (int i = 0; i< count; i++) {
            list.add(cur);
            cur += step;
        }
        return list;
    }

    private static ToweredTypeUtils<String, TableRowStrIntCols> getTableStrIntColsIndexTypeUtils() {
        ITypeUtils<String> stringClassUtils = new StringClassUtils();
        ITypeUtils<TableRowStrIntCols> tableRowStrIntColsClassUtils = new TableRowStrIntColsClassUtils();

        return new ToweredTypeUtils<>(stringClassUtils, tableRowStrIntColsClassUtils);
    }

    private static ToweredTypeUtils<Integer, TableRowIntDateCols> getTableIntDateColsIndexTypeUtils() {
        ITypeUtils<Integer> integerClassUtils = new IntegerClassUtils();
        ITypeUtils<TableRowIntDateCols> tableRowIntDateColsClassUtils = new TableRowIntDateColsClassUtils();

        return new ToweredTypeUtils<>(integerClassUtils, tableRowIntDateColsClassUtils);
    }

    private static ToweredTypeUtils<String, TableRowStrLongCols> getTableStrLongColsIndexTypeUtils() {
        ITypeUtils<String> stringClassUtils = new StringClassUtils();
        ITypeUtils<TableRowStrLongCols> tableRowStrLongColsClassUtils = new TableRowStrLongColsClassUtils();

        return new ToweredTypeUtils<>(stringClassUtils, tableRowStrLongColsClassUtils);
    }

    private static ArrayList<TableRowStrIntCols> getTableRowStrIntCols(ArrayList<Integer> intArr, ArrayList<String> strArr, ITypeUtils<TableRowStrIntCols> tableRowStrIntColsClassUtils) {
        ArrayList<TableRowStrIntCols> trs = new ArrayList<>();
        for (int i = 0; i < intArr.size(); i++) {
            String col1 = strArr.get(i);
            int col2 = intArr.get(i);
            TableRowStrIntCols tr = new TableRowStrIntCols(col1, col2, tableRowStrIntColsClassUtils);
//            Version<Integer> v = tr.getVersion();
//            v.setCreateVersion(0);
            trs.add(tr);
        }
        return trs;
    }

    public static Map<String, Object> tableStrIntColsSkipListIndexScenario(ArrayList<Integer> intArr, ArrayList<String> strArr, double iterationProbability, int partitionCapacity) throws Exception {
        System.out.println("tableStrIntColsSkipListIndex");
        Utils.assertTrue(intArr.size() == strArr.size());

        MyTimer myTimer = new MyTimer();
        ToweredTypeUtils<String, TableRowStrIntCols> tableStrIntColsIndexTypeUtils = getTableStrIntColsIndexTypeUtils();
        ToweredSkipList<Integer, String, TableRowStrIntCols> tableStrIntColsIndex = new ToweredSkipList<>(0, iterationProbability, partitionCapacity, tableStrIntColsIndexTypeUtils);
        ITypeUtils<TableRowStrIntCols> tableRowStrIntColsClassUtils = tableStrIntColsIndexTypeUtils.vTypeUtils;

        System.out.println("tableStrIntColsSkipListIndex insert start");
        ArrayList<TableRowStrIntCols> trs = getTableRowStrIntCols(intArr, strArr, tableRowStrIntColsClassUtils);

        myTimer.start();

        for (TableRowStrIntCols tr : trs) {
            tableStrIntColsIndex.upsert(tr);
        }

        myTimer.pause();
        double insertLs = myTimer.getElapsedSeconds();
        double insertTPs = ((double) trs.size()) / ((double) myTimer.getElapsedSeconds());
        System.out.println("tableStrIntColsSkipListIndex insert time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("tableStrIntColsSkipListIndex update start");
        trs = getTableRowStrIntCols(intArr, strArr, tableRowStrIntColsClassUtils);

        myTimer.init();
        myTimer.start();

//        for (TableRowStrIntCols tr : trs) {
//            tableStrIntColsIndex.upsert(tr);
//        }

        myTimer.pause();
        double updateLs = myTimer.getElapsedSeconds();
        double updateTPs = ((double) trs.size()) / ((double) myTimer.getElapsedSeconds());
        System.out.println("tableStrIntColsSkipListIndex update time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("tableStrIntColsSkipListIndex search start");
        myTimer.init();
        myTimer.start();

        for (String key : strArr) {
            TableRowStrIntCols s = tableStrIntColsIndex.search1(0, key);
        }

        myTimer.pause();
        double searchLms = myTimer.getElapsedMilliSeconds();
        double searchTPs = ((double) strArr.size()) / ((double) myTimer.getElapsedSeconds());
        System.out.println("tableStrIntColsSkipListIndex search time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("tableStrIntColsSkipListIndex range all start");
        ArrayList<IRowDetails<String, TableRowStrIntCols,Integer>> s = new ArrayList<>(intArr.size());

        myTimer.init();
        myTimer.start();

        tableStrIntColsIndex.rangeSearch1(0, "", "ZZZZZZZZZ",s);

        myTimer.pause();
        double rangeAllKeysLms = myTimer.getElapsedMilliSeconds();
        double rangeAllKeysTPs = ((double) s.size()) / ((double) myTimer.getElapsedSeconds());
        System.out.println("tableStrIntColsSkipListIndex range all time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("tableStrIntColsSkipListIndex delete start");
        myTimer.init();
        myTimer.start();

//        for (int i = 0; i < count; i++) {
//            String d = strArr.get(i);
//            tableStrIntColsIndex.delete(d);
//        }

        myTimer.pause();
        double deleteLms = myTimer.getElapsedMilliSeconds();
        double deleteTPs = ((double) intArr.size()) / ((double) myTimer.getElapsedSeconds());
        System.out.println("tableStrIntColsSkipListIndex delete time (ns): " + myTimer.getElapsedNanoSeconds());

        return getStringObjectMapScenario2(intArr, insertLs, insertTPs, updateLs, updateTPs, searchLms, searchTPs, rangeAllKeysLms, rangeAllKeysTPs, deleteLms, deleteTPs);
    }

    public static Map<String, Object> patriciaMerkleTrieIndexScenario(ArrayList<Integer> intArr, ArrayList<String> strArr) throws Exception {
        System.out.println("patriciaMerkleTrieIndex");
        Utils.assertTrue(intArr.size() == strArr.size());

        MyTimer myTimer = new MyTimer();
        ITypeUtils<Integer> integerClassUtils = new IntegerClassUtils();
        Trie<String,Integer> mpt = new Trie<>(Serializer.STRING_UTF8, integerClassUtils);

        System.out.println("patriciaMerkleTrieIndex insert start");
        myTimer.start();

        for (int i = 0; i < intArr.size(); i++) {
            mpt.put(strArr.get(i), intArr.get(i));
        }

        myTimer.pause();
        double insertLs = myTimer.getElapsedSeconds();
        double insertTPs = ((double) intArr.size()) / ((double) myTimer.getElapsedSeconds());
        System.out.println("patriciaMerkleTrieIndex insert time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("patriciaMerkleTrieIndex update start");
        myTimer.init();
        myTimer.start();

//        for (int i = 0; i < intArr.size(); i++) {
//            mpt.put(strArr.get(i), intArr.get(i));
//        }

        myTimer.pause();
        double updateLs = myTimer.getElapsedSeconds();
        double updateTPs = ((double) intArr.size()) / ((double) myTimer.getElapsedSeconds());
        System.out.println("patriciaMerkleTrieIndex update time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("patriciaMerkleTrieIndex search start");
        myTimer.init();
        myTimer.start();

        for (int i = 0; i < strArr.size(); i++) {
            Object result = mpt.get(strArr.get(i));
        }

        myTimer.pause();
        double searchLms = myTimer.getElapsedMilliSeconds();
        double searchTPs = ((double) strArr.size()) / ((double) myTimer.getElapsedSeconds());
        System.out.println("patriciaMerkleTrieIndex search time (ns): " + myTimer.getElapsedNanoSeconds());

        Map<String,Object> res = new HashMap<>();
        res.put("count", intArr.size());
        res.put("insertLs",insertLs);
        res.put("insertTPs",insertTPs);
        res.put("updateLs",updateLs);
        res.put("updateTPs",updateTPs);
        res.put("searchLms",searchLms);
        res.put("searchTPs",searchTPs);

        return res;
    }

    public static Map<String, Object> InsertMVScenario(String indexName, IIndexMVIntDate index, List<TableRowIntDateCols> data) throws Exception {
        System.out.println(indexName);

        MyTimer myTimer = new MyTimer();

        System.out.println(indexName + "insert start");
        myTimer.start();

        for (TableRowIntDateCols row : data) {
            index.insert(row);
        }

        myTimer.pause();
        double insertLs = myTimer.getElapsedSeconds();
        double insertTPs = ((double) data.size()) / ((double) myTimer.getElapsedSeconds());
        System.out.println(indexName + "insert end. time (s): " + myTimer.getElapsedSeconds());

        Map<String,Object> res = new HashMap<>();
        res.put("count", data.size());
        res.put("insertLs",insertLs);
        res.put("insertTPs",insertTPs);

        return res;
    }

    private static void collectEvaluationResultsScenario(String logPrefix, String logPostfix, int dataStart, int dataEnd, int dataInc, int runs, boolean initFiles) throws Exception {
        HashMap<String, HashMap<String, Object>> filesMetaData = new HashMap<>();
        addEntry(merkleBucketTree, "count,capacity,insertLs,insertTPs", filesMetaData);
        addEntry(patriciaMerkleTrie, "count,insertLs,insertTPs", filesMetaData);
        addEntry(temporalSkipList, "count,maxLevel,insertLs,insertTPs", filesMetaData);
        addEntry(verToIndex, "count,maxLevel,insertLs,insertTPs", filesMetaData);
        addEntry(temporalIndex, "count,maxLevel,insertLs,insertTPs", filesMetaData);
        addEntry(phTreeIndex, "count,insertLs,insertTPs", filesMetaData);

        collectEvaluationResultsScenario(logPrefix, logPostfix, dataStart, dataEnd, dataInc, runs, filesMetaData, initFiles);

        ProcessRunsAvg(Paths.get(logPrefix + merkleBucketTree + logPostfix),
                Paths.get(logPrefix + merkleBucketTree + runsAvgProcessed + logPostfix),
                filesMetaData.get(merkleBucketTree), new HashSet<>(Arrays.asList(0,1)));
        ProcessRunsAvg(Paths.get(logPrefix + patriciaMerkleTrie + logPostfix),
                Paths.get(logPrefix + patriciaMerkleTrie + runsAvgProcessed + logPostfix),
                filesMetaData.get(patriciaMerkleTrie), new HashSet<>(Arrays.asList(0)));
        ProcessRunsAvg(Paths.get(logPrefix + temporalSkipList + logPostfix),
                Paths.get(logPrefix + temporalSkipList + runsAvgProcessed + logPostfix),
                filesMetaData.get(temporalSkipList), new HashSet<>(Arrays.asList(0,1)));
        ProcessRunsAvg(Paths.get(logPrefix + verToIndex + logPostfix),
                Paths.get(logPrefix + verToIndex + runsAvgProcessed + logPostfix),
                filesMetaData.get(verToIndex), new HashSet<>(Arrays.asList(0,1)));
        ProcessRunsAvg(Paths.get(logPrefix + temporalIndex + logPostfix),
                Paths.get(logPrefix + temporalIndex + runsAvgProcessed + logPostfix),
                filesMetaData.get(temporalIndex), new HashSet<>(Arrays.asList(0,1)));
        ProcessRunsAvg(Paths.get(logPrefix + phTreeIndex + logPostfix),
                Paths.get(logPrefix + phTreeIndex + runsAvgProcessed + logPostfix),
                filesMetaData.get(phTreeIndex), new HashSet<>(Arrays.asList(0)));

    }

    public static void collectEvaluationResultsScenario(String logPrefix, String logPostfix, int dataStart, int dataEnd, int dataInc, int runs, HashMap<String, HashMap<String, Object>> filesMetaData, boolean initFiles) throws Exception {
        Path pathMerkleBucket = Paths.get(logPrefix + merkleBucketTree + logPostfix);
        Path pathPatriciaMerkleTrie = Paths.get(logPrefix + patriciaMerkleTrie + logPostfix);
        Path pathTempSkipList = Paths.get(logPrefix + temporalSkipList + logPostfix);
        Path pathVerToIndex = Paths.get(logPrefix + verToIndex + logPostfix);
        Path pathTempIndex = Paths.get(logPrefix + temporalIndex + logPostfix);
        Path pathPHTreeIndex = Paths.get(logPrefix + phTreeIndex + logPostfix);

        if (initFiles) {
            initFile(merkleBucketTree, filesMetaData, pathMerkleBucket);
            initFile(patriciaMerkleTrie, filesMetaData, pathPatriciaMerkleTrie);
            initFile(temporalSkipList, filesMetaData, pathTempSkipList);
            initFile(verToIndex, filesMetaData, pathVerToIndex);
            initFile(temporalIndex, filesMetaData, pathTempIndex);
            initFile(phTreeIndex, filesMetaData, pathPHTreeIndex);
        }

        ArrayList<HashMap<String, Object>> settings = getSettings(dataStart, dataEnd, dataInc, runs);

        ToweredTypeUtils<Integer, TableRowIntDateCols> tableIntDateColsIndexTypeUtils = getTableIntDateColsIndexTypeUtils();

        for (HashMap<String, Object> settingsRow : settings) {
            int count = (int) settingsRow.get("count");
            System.out.println("collectEvaluationResults - count: " + count);

            int totalRowsCount = count;
            LocalDate startLocalDate = LocalDate.of(2015, Month.JANUARY, 1);
            int datesCount = 1000;
            int firstPatientID = 1;
            double patientsPerDateRatio = 0.8;

            ArrayList<Integer> merkleCapacities = (ArrayList<Integer>) settingsRow.get("merkleCapacities");
            ArrayList<Integer> temporalIndexCapacities = (ArrayList<Integer>) settingsRow.get("temporalIndexCapacities");

            for (int merkleCapacity : merkleCapacities) {

                ArrayList<TupleTwo<Integer, Date>> data = getSyntheticMedicalDataSet(totalRowsCount, startLocalDate, datesCount, firstPatientID, patientsPerDateRatio);
                List<TableRowIntDateCols> data_ = getTableRowIntDateCols(data, tableIntDateColsIndexTypeUtils);

                IIndexMVIntDate index = new MerkleBucketTreeMVIntDateWrapper(merkleCapacity);
                Map<String, Object> merkleBucketIndexRunRes = InsertMVScenario("merkleBucketTreeIndex", index, data_);
                String line = String.format("%s,%s,%s,%s",
                        merkleBucketIndexRunRes.get("count"), merkleCapacity,
                        merkleBucketIndexRunRes.get("insertLs"), merkleBucketIndexRunRes.get("insertTPs"));
                Utils.writeToFile(pathMerkleBucket, line + System.lineSeparator(), StandardOpenOption.APPEND);
            }

            for (int i = 0; i < runs; i++) {
                ArrayList<TupleTwo<Integer, Date>> data = getSyntheticMedicalDataSet(totalRowsCount, startLocalDate, datesCount, firstPatientID, patientsPerDateRatio);
                List<TableRowIntDateCols> data_ = getTableRowIntDateCols(data, tableIntDateColsIndexTypeUtils);

                IIndexMVIntDate index = new PatriciaMerkleTrieMVIntDateWrapper();
                Map<String, Object> pmtIndexRunRes = InsertMVScenario("patriciaMerkleTrieIndex", index, data_);
                String line = String.format("%s,%s,%s",
                        pmtIndexRunRes.get("count"), pmtIndexRunRes.get("insertLs"), pmtIndexRunRes.get("insertTPs"));
                Utils.writeToFile(pathPatriciaMerkleTrie, line + System.lineSeparator(), StandardOpenOption.APPEND);
            }

            for (int temporalIndexCapacity : temporalIndexCapacities) {
                ArrayList<TupleTwo<Integer, Date>> data = getSyntheticMedicalDataSet(totalRowsCount, startLocalDate, datesCount, firstPatientID, patientsPerDateRatio);

                HashSet<Date> versions_ = new HashSet<>();
                for (TupleTwo<Integer, Date> row : data) {
                    versions_.add(row.second);
                }

                List<Date> versions = versions_.stream()
                        .sorted(Comparator.comparing(o -> o))
                        .collect(Collectors.toList());

                Date firstVersion = versions.get(0);

                List<TableRowIntDateCols> data_ = getTableRowIntDateCols(data, tableIntDateColsIndexTypeUtils);

                ToweredSkipList.MAX_LEVEL = temporalIndexCapacity;
                IVersionsToKeysIndex<Date, Integer> versionsToKeysIndex = new VersionsToKeysIndex<>(firstVersion, data.size());
                IIndexMVIntDate index = new SkipListMVIntDateWrapper(firstVersion, iterationProbability, versionsToKeysIndex, partitionCapacity, tableIntDateColsIndexTypeUtils);
                Map<String, Object> skipListIndexRunRes = InsertMVScenario("tableStrIntColsSkipListIndex", index, data_);
                String line = String.format("%s,%s,%s,%s",
                        skipListIndexRunRes.get("count"), ToweredSkipList.MAX_LEVEL,
                        skipListIndexRunRes.get("insertLs"), skipListIndexRunRes.get("insertTPs"));
                Utils.writeToFile(pathTempSkipList, line + System.lineSeparator(), StandardOpenOption.APPEND);


//                index = new PHTreeIntDateWrapper();
//                Map<String, Object> phTreeIndexRunRes = InsertMVScenario("tableStrIntColsPHTreeIndexScenario", index, data_);
//                line = String.format("%s,%s,%s", phTreeIndexRunRes.get("count"), phTreeIndexRunRes.get("insertLs"), phTreeIndexRunRes.get("insertTPs"));
//                Utils.writeToFile(pathPHTreeIndex, line + System.lineSeparator(), StandardOpenOption.APPEND);


//                Map<String, Object> verToKeysIndexRunRes = tableStrIntColsVerToKeysIndex(strArr);
//                line = String.format("%s,%s,%s,%s,%s,%s,%s,%s",
//                        verToKeysIndexRunRes.get("count"), verToKeysIndexRunRes.get("maxLevel"),
//                        verToKeysIndexRunRes.get("insertLs"), verToKeysIndexRunRes.get("insertTPs"),
//                        verToKeysIndexRunRes.get("firstVersionSearchLms"), verToKeysIndexRunRes.get("firstVersionSearchTPs"),
//                        verToKeysIndexRunRes.get("deleteLms"), verToKeysIndexRunRes.get("deleteTPs"));
//                Utils.writeToFile(pathVerToIndex, line + System.lineSeparator(), StandardOpenOption.APPEND);


                changeKeysToConsecutiveKeys(data);
                data_ = getTableRowIntDateCols(data, tableIntDateColsIndexTypeUtils);

                ToweredSkipList.MAX_LEVEL = temporalIndexCapacity;
                VersionsToConsecutiveKeysIndex<Date, Integer> versionsToConsecutiveKeysIndex = new VersionsToConsecutiveKeysIndex<>(firstVersion, data.size());
                index = new SkipListMVIntDateWrapper(firstVersion, iterationProbability, versionsToConsecutiveKeysIndex, partitionCapacity, tableIntDateColsIndexTypeUtils);

                Map<String, Object> tempIndexRunRes = InsertMVScenario("tableStrIntColsTemporalIndexScenario", index, data_);
                line = String.format("%s,%s,%s,%s",
                        tempIndexRunRes.get("count"), ToweredSkipList.MAX_LEVEL,
                        tempIndexRunRes.get("insertLs"), tempIndexRunRes.get("insertTPs"));
                Utils.writeToFile(pathTempIndex, line + System.lineSeparator(), StandardOpenOption.APPEND);
            }

        }
    }

    private static List<TableRowIntDateCols> getTableRowIntDateCols(ArrayList<TupleTwo<Integer, Date>> data, ToweredTypeUtils<Integer, TableRowIntDateCols> tableIntDateColsIndexTypeUtils) {
        List<TableRowIntDateCols> data_ = new ArrayList<>(data.size());
        for (TupleTwo<Integer, Date> row : data) {
            TableRowIntDateCols tr = new TableRowIntDateCols(row.first, row.second, tableIntDateColsIndexTypeUtils.vTypeUtils);
            data_.add(tr);
        }
        return data_;
    }

    private static void changeKeysToConsecutiveKeys(ArrayList<TupleTwo<Integer, Date>> data) {

        HashMap<Integer,Integer> keysToMappedKeys = new HashMap<>();

        for (TupleTwo<Integer, Date> row : data) {
            int key = row.first;
            Integer mappedKey = keysToMappedKeys.getOrDefault(key, null);
            if (mappedKey == null) {
                mappedKey = keysToMappedKeys.size();
                keysToMappedKeys.put(key, mappedKey);
            }
            row.first = mappedKey;
        }
    }

    private static void runScenario2(String logPrefix, String logPostfix, HashMap<String, HashMap<String, Object>> filesMetaData, ArrayList<Integer> itemsCounts, ArrayList<Integer> versionsCounts, int runs, ArrayList<Double> keysPercentsCounts) throws Exception {

        HashMap<String, Object> settings = new HashMap<>();

        settings.put("runsCount", runs);
        settings.put("MBTCapacity", 10000); // This? 
        settings.put("temporalIndexCapacity", 20); // This? 

        boolean isFirstBatch = true;
        for (Integer itemsCount: itemsCounts) {
            for (Integer versionsCount : versionsCounts) {
                for (double keysPercent : keysPercentsCounts) {
                    settings.put("percent", keysPercent);
                    settings.put("versionsCount", versionsCount);
                    settings.put("itemsCount", itemsCount);

                    System.out.println("runScenario - " + logPrefix + logPostfix + " itemsCount:" + itemsCount + " versionsCount: " + versionsCount + " keysPercent:" + keysPercent);

                    collectEvaluationResultsScenario2(logPrefix, logPostfix, settings, filesMetaData, isFirstBatch);
                    isFirstBatch = false;
                }
            }
        }
    }



    private static void collectEvaluationResultsScenario2(String logPrefix, String logPostfix, ArrayList<Integer> itemsCounts, ArrayList<Integer> versionsCounts, int runs, ArrayList<Double> keysPercentsCounts) throws Exception {
        HashMap<String, HashMap<String, Object>> filesMetaData = new HashMap<>();
        addEntry(merkleBucketTree, "count,versions,keysPercent,insertLs,insertTPs,rangeSearch1SkipListLns,rangeSearch1MerkleLns,rangeSearch3SkipListLms,rangeSearch3MerkleLms,rangeSearch4SkipListLms,rangeSearch4MerkleLms", filesMetaData);
        addEntry(patriciaMerkleTrie, "count,versions,keysPercent,insertLs,insertTPs,rangeSearch1SkipListLns,rangeSearch1MerkleLns,rangeSearch3SkipListLms,rangeSearch3MerkleLms,rangeSearch4SkipListLms,rangeSearch4MerkleLms", filesMetaData);
        addEntry(temporalSkipList, "count,versions,keysPercent,insertLs,insertTPs,rangeSearch1SkipListLns,rangeSearch1MerkleLns,rangeSearch3SkipListLms,rangeSearch3MerkleLms,rangeSearch4SkipListLms,rangeSearch4MerkleLms", filesMetaData);
        addEntry(temporalIndex, "count,versions,keysPercent,insertLs,insertTPs,rangeSearch1SkipListLns,rangeSearch1MerkleLns,rangeSearch3SkipListLms,rangeSearch3MerkleLms,rangeSearch4SkipListLms,rangeSearch4MerkleLms", filesMetaData);
        addEntry(phTreeIndex, "count,versions,keysPercent,insertLs,insertTPs,rangeSearch1SkipListLns,rangeSearch1MerkleLns,rangeSearch3SkipListLms,rangeSearch3MerkleLms,rangeSearch4SkipListLms,rangeSearch4MerkleLms", filesMetaData);
        addEntry(merkleKDTreeIndex, "count,versions,keysPercent,insertLs,insertTPs,rangeSearch1SkipListLns,rangeSearch1MerkleLns,rangeSearch3SkipListLms,rangeSearch3MerkleLms,rangeSearch4SkipListLms,rangeSearch4MerkleLms", filesMetaData);

        runScenario2(logPrefix, "Scenario2" + logPostfix, filesMetaData, itemsCounts, versionsCounts, runs, keysPercentsCounts);

        ProcessRunsAvg(Paths.get(logPrefix + merkleBucketTree + "Scenario2" + logPostfix),
                Paths.get(logPrefix + merkleBucketTree + "Scenario2" + runsAvgProcessed + logPostfix),
                filesMetaData.get(merkleBucketTree), new HashSet<>(Arrays.asList(0, 1, 2)));
        ProcessRunsAvg(Paths.get(logPrefix + patriciaMerkleTrie + "Scenario2" + logPostfix),
                Paths.get(logPrefix + patriciaMerkleTrie + "Scenario2" + runsAvgProcessed + logPostfix),
                filesMetaData.get(patriciaMerkleTrie), new HashSet<>(Arrays.asList(0, 1, 2)));
        ProcessRunsAvg(Paths.get(logPrefix + temporalSkipList + "Scenario2" + logPostfix),
                Paths.get(logPrefix + temporalSkipList + "Scenario2" + runsAvgProcessed + logPostfix),
                filesMetaData.get(temporalSkipList), new HashSet<>(Arrays.asList(0, 1, 2)));
        ProcessRunsAvg(Paths.get(logPrefix + temporalIndex + "Scenario2" + logPostfix),
                Paths.get(logPrefix + temporalIndex + "Scenario2" + runsAvgProcessed + logPostfix),
                filesMetaData.get(temporalIndex), new HashSet<>(Arrays.asList(0, 1, 2)));
        ProcessRunsAvg(Paths.get(logPrefix + phTreeIndex + "Scenario2" + logPostfix),
                Paths.get(logPrefix + phTreeIndex + "Scenario2" + runsAvgProcessed + logPostfix),
                filesMetaData.get(phTreeIndex), new HashSet<>(Arrays.asList(0, 1, 2)));
        ProcessRunsAvg(Paths.get(logPrefix + merkleKDTreeIndex + "Scenario2" + logPostfix),
                Paths.get(logPrefix + merkleKDTreeIndex + "Scenario2" + runsAvgProcessed + logPostfix),
                filesMetaData.get(merkleKDTreeIndex), new HashSet<>(Arrays.asList(0, 1, 2)));
    }


    private static void collectEvaluationResultsScenario33(String logPrefix, String logPostfix, double collectEvaluationResults3keysPercentsCounts, CsvReader csvReader) throws Exception {
        HashMap<String, HashMap<String, Object>> filesMetaData = new HashMap<>();
        addEntry(merkleBucketTree, "count,versions,keysPercent,insertLs,insertTPs,rangeSearch1SkipListLns,rangeSearch1MerkleLns,rangeSearch3SkipListLms,rangeSearch3MerkleLms,rangeSearch4SkipListLms,rangeSearch4MerkleLms", filesMetaData);
        addEntry(patriciaMerkleTrie, "count,versions,keysPercent,insertLs,insertTPs,rangeSearch1SkipListLns,rangeSearch1MerkleLns,rangeSearch3SkipListLms,rangeSearch3MerkleLms,rangeSearch4SkipListLms,rangeSearch4MerkleLms", filesMetaData);
        addEntry(temporalSkipList, "count,versions,keysPercent,insertLs,insertTPs,rangeSearch1SkipListLns,rangeSearch1MerkleLns,rangeSearch3SkipListLms,rangeSearch3MerkleLms,rangeSearch4SkipListLms,rangeSearch4MerkleLms", filesMetaData);
        addEntry(temporalIndex, "count,versions,keysPercent,insertLs,insertTPs,rangeSearch1SkipListLns,rangeSearch1MerkleLns,rangeSearch3SkipListLms,rangeSearch3MerkleLms,rangeSearch4SkipListLms,rangeSearch4MerkleLms", filesMetaData);


        HashMap<String, Object> settings = new HashMap<>();
        settings.put("MBTCapacity", 1000);
        settings.put("temporalIndexCapacity", 20);
        settings.put("percent", collectEvaluationResults3keysPercentsCounts);


        collectEvaluationResultsScenario33(logPrefix, "Scenario33" + logPostfix, settings, filesMetaData, csvReader);


        ProcessRunsAvg(Paths.get(logPrefix + merkleBucketTree + "Scenario33" + logPostfix),
                Paths.get(logPrefix + merkleBucketTree + "Scenario33" + runsAvgProcessed + logPostfix),
                filesMetaData.get(merkleBucketTree), new HashSet<>(Arrays.asList(0, 1, 2)));
        ProcessRunsAvg(Paths.get(logPrefix + patriciaMerkleTrie + "Scenario33" + logPostfix),
                Paths.get(logPrefix + patriciaMerkleTrie + "Scenario33" + runsAvgProcessed + logPostfix),
                filesMetaData.get(patriciaMerkleTrie), new HashSet<>(Arrays.asList(0, 1, 2)));
        ProcessRunsAvg(Paths.get(logPrefix + temporalSkipList + "Scenario33" + logPostfix),
                Paths.get(logPrefix + temporalSkipList + "Scenario33" + runsAvgProcessed + logPostfix),
                filesMetaData.get(temporalSkipList), new HashSet<>(Arrays.asList(0, 1, 2)));
        ProcessRunsAvg(Paths.get(logPrefix + temporalIndex + "Scenario33" + logPostfix),
                Paths.get(logPrefix + temporalIndex + "Scenario33" + runsAvgProcessed + logPostfix),
                filesMetaData.get(temporalIndex), new HashSet<>(Arrays.asList(0, 1, 2)));
    }

    public static void collectEvaluationResultsScenario2(String logPrefix, String logPostfix, HashMap<String, Object> settings, HashMap<String,HashMap<String,Object>> filesMetaData, boolean initCsvFiles) throws Exception {

        // Path variables
        Path pathMerkleBucketScenario = Paths.get(logPrefix + merkleBucketTree + logPostfix);
        Path pathPatriciaMerkleTrieScenario = Paths.get(logPrefix + patriciaMerkleTrie + logPostfix);
        Path pathTempSkipListScenario = Paths.get(logPrefix + temporalSkipList + logPostfix);
        Path pathTempIndexScenario = Paths.get(logPrefix + temporalIndex + logPostfix);
        Path pathPhTreeIndexScenario = Paths.get(logPrefix + phTreeIndex + logPostfix);
        Path pathMerkleKDTreeIndexScenario = Paths.get(logPrefix + merkleKDTreeIndex + logPostfix);

         // Initialize files, write headers;  
        if (initCsvFiles) {
            initFile(merkleBucketTree, filesMetaData, pathMerkleBucketScenario);
            initFile(patriciaMerkleTrie, filesMetaData, pathPatriciaMerkleTrieScenario);
            initFile(temporalSkipList, filesMetaData, pathTempSkipListScenario);
            initFile(temporalIndex, filesMetaData, pathTempIndexScenario);
            initFile(phTreeIndex, filesMetaData, pathPhTreeIndexScenario);
            initFile(merkleKDTreeIndex, filesMetaData, pathMerkleKDTreeIndexScenario);
        }

        int runsCount = (int) settings.get("runsCount");
        int itemsCount = (int) settings.get("itemsCount");
        int versionsCount = (int) settings.get("versionsCount");
        int MBTCapacity = (int) settings.get("MBTCapacity");
        int temporalIndexCapacity = (int) settings.get("temporalIndexCapacity");
        double percent = (double) settings.get("percent");


        int totalRowsCount = itemsCount;
        LocalDate startLocalDate = LocalDate.of(2015, Month.JANUARY, 1);
        int datesCount = versionsCount;
        int firstPatientID = 1;
        double patientsPerDateRatio = 0.8;

        ToweredTypeUtils<Integer, TableRowIntDateCols> tableIntDateColsIndexTypeUtils = getTableIntDateColsIndexTypeUtils();

        for (int runs = 0; runs < runsCount; runs++) {
            // data = List<patientID, date> 
            ArrayList<TupleTwo<Integer, Date>> data = getSyntheticMedicalDataSet(totalRowsCount, startLocalDate, datesCount, firstPatientID, patientsPerDateRatio);

            HashSet<Date> versions_ = new HashSet<>();
            for (TupleTwo<Integer, Date> row : data) {
                versions_.add(row.second); // versions_ is a set/list of dates
            }

            // sorting versions_
            List<Date> versions = versions_.stream()
                    .sorted(Comparator.comparing(o -> o))
                    .collect(Collectors.toList());

            Date firstVersion = versions.get(0); // Each version is a date

            // Wrapping each row in data with <TableRowIntDateCols> class
            List<TableRowIntDateCols> data_ = getTableRowIntDateCols(data, tableIntDateColsIndexTypeUtils);

            IIndexMVIntDate index = null;
//            index = new PatriciaMerkleTrieMVIntDateWrapper();
//            Map<String, Object> patriciaMerkleTrieRunRes = SearchMVScenarios("patriciaMerkleTrieIndex", index, data_, percent);
//            writeScenarioResultsToFile(pathPatriciaMerkleTrieScenario, patriciaMerkleTrieRunRes);

//            index = new MerkleBucketTreeMVIntDateWrapper(MBTCapacity);
//            Map<String, Object> merkleBucketIndexRunRes = SearchMVScenarios("merkleBucketTreeIndex", index, data_, percent);
//            writeScenarioResultsToFile(pathMerkleBucketScenario, merkleBucketIndexRunRes);

            // Where is this coming from? 
            ToweredSkipList.MAX_LEVEL = temporalIndexCapacity;
            IVersionsToKeysIndex<Date, Integer> versionsToKeysIndex = new VersionsToKeysIndex<>(firstVersion, data.size());
            index = new SkipListMVIntDateWrapper(firstVersion, iterationProbability, versionsToKeysIndex, partitionCapacity, tableIntDateColsIndexTypeUtils);
            Map<String, Object> skipListIndexRunRes = SearchMVScenarios("tableStrIntColsSkipListIndex", index, data_, percent);
            writeScenarioResultsToFile(pathTempSkipListScenario, skipListIndexRunRes);

            // AVL Index Code

            index = new PHTreeIntDateWrapper();
            Map<String, Object> phTreeIndexRunRes = SearchMVScenarios("phTreeIndex", index, data_, percent);
            writeScenarioResultsToFile(pathPhTreeIndexScenario, phTreeIndexRunRes);

            index = new MerkleKDTreeIntDateWrapper();
            Map<String, Object> merkleKDTreeIndexRunRes = SearchMVScenarios("merkleKDTreeIndex", index, data_, percent);
            writeScenarioResultsToFile(pathMerkleKDTreeIndexScenario, merkleKDTreeIndexRunRes);





            // TODO find scenario to strictly increasing keys only, which can use VersionsToConsecutiveKeysIndex
//            changeKeysToConsecutiveKeys(data);
//            data_ = getTableRowIntDateCols(data, tableIntDateColsIndexTypeUtils);
//
//            ToweredSkipList.MAX_LEVEL = temporalIndexCapacity;
//            VersionsToConsecutiveKeysIndex<Date, Integer> versionsToConsecutiveKeysIndex = new VersionsToConsecutiveKeysIndex<>(firstVersion, data.size());
//            index = new SkipListMVIntDateWrapper(firstVersion, iterationProbability, versionsToConsecutiveKeysIndex, partitionCapacity, tableIntDateColsIndexTypeUtils);
//            Map<String, Object> tempIndexRunRes = SearchMVScenarios("tableStrIntColsTemporalIndexScenario", index, data_, percent);
//            writeScenarioResultsToFile(pathTempIndexScenario, tempIndexRunRes);

        }
    }

    public static void collectEvaluationResultsScenario33(String logPrefix, String logPostfix, HashMap<String, Object> settings, HashMap<String,HashMap<String,Object>> filesMetaData, CsvReader csvReader) throws Exception {

        Path pathMerkleBucketScenario = Paths.get(logPrefix + merkleBucketTree + logPostfix);
        Path pathPatriciaMerkleTrieScenario = Paths.get(logPrefix + patriciaMerkleTrie + logPostfix);
        Path pathTempSkipListScenario = Paths.get(logPrefix + temporalSkipList + logPostfix);
        Path pathTempIndexScenario = Paths.get(logPrefix + temporalIndex + logPostfix);

        initFile(merkleBucketTree, filesMetaData, pathMerkleBucketScenario);
        initFile(patriciaMerkleTrie, filesMetaData, pathPatriciaMerkleTrieScenario);
        initFile(temporalSkipList, filesMetaData, pathTempSkipListScenario);
        initFile(temporalIndex, filesMetaData, pathTempIndexScenario);

        int MBTCapacity = (int) settings.get("MBTCapacity");
        int temporalIndexCapacity = (int) settings.get("temporalIndexCapacity");
        double percent = (double) settings.get("percent");

        ToweredTypeUtils<Integer, TableRowIntDateCols> tableIntDateColsIndexTypeUtils = getTableIntDateColsIndexTypeUtils();


        ArrayList<TupleTwo<Integer, Date>> data = getMedicalDataSet(csvReader);

        HashSet<Date> versions_ = new HashSet<>();
        for (TupleTwo<Integer, Date> row : data) {
            versions_.add(row.second);
        }

        List<Date> versions = versions_.stream()
                .sorted(Comparator.comparing(o -> o))
                .collect(Collectors.toList());

        Date firstVersion = versions.get(0);

        List<TableRowIntDateCols> data_ = getTableRowIntDateCols(data, tableIntDateColsIndexTypeUtils);

        IIndexMVIntDate index = null;
        index = new PatriciaMerkleTrieMVIntDateWrapper();
        Map<String, Object> patriciaMerkleTrieRunRes = SearchMVScenarios("patriciaMerkleTrieIndex", index, data_, percent);
        writeScenarioResultsToFile(pathPatriciaMerkleTrieScenario, patriciaMerkleTrieRunRes);

        index = new MerkleBucketTreeMVIntDateWrapper(MBTCapacity);
        Map<String, Object> merkleBucketIndexRunRes = SearchMVScenarios("merkleBucketTreeIndex", index, data_, percent);
        writeScenarioResultsToFile(pathMerkleBucketScenario, merkleBucketIndexRunRes);

        ToweredSkipList.MAX_LEVEL = temporalIndexCapacity;
        IVersionsToKeysIndex<Date, Integer> versionsToKeysIndex = new VersionsToKeysIndex<>(firstVersion, data.size());
        index = new SkipListMVIntDateWrapper(firstVersion, iterationProbability, versionsToKeysIndex, partitionCapacity, tableIntDateColsIndexTypeUtils);
        Map<String, Object> skipListIndexRunRes = SearchMVScenarios("tableStrIntColsSkipListIndex", index, data_, percent);
        writeScenarioResultsToFile(pathTempSkipListScenario, skipListIndexRunRes);

        // TODO find scenario to strictly increasing keys only, which can use VersionsToConsecutiveKeysIndex
        changeKeysToConsecutiveKeys(data);
        data_ = getTableRowIntDateCols(data, tableIntDateColsIndexTypeUtils);

        ToweredSkipList.MAX_LEVEL = temporalIndexCapacity;
        VersionsToConsecutiveKeysIndex<Date, Integer> versionsToConsecutiveKeysIndex = new VersionsToConsecutiveKeysIndex<>(firstVersion, data.size());
        index = new SkipListMVIntDateWrapper(firstVersion, iterationProbability, versionsToConsecutiveKeysIndex, partitionCapacity, tableIntDateColsIndexTypeUtils);
        Map<String, Object> tempIndexRunRes = SearchMVScenarios("tableStrIntColsTemporalIndexScenario", index, data_, percent);
        writeScenarioResultsToFile(pathTempIndexScenario, tempIndexRunRes);
    }

    public static Map<String, Object> SearchMVScenarios(String indexName, IIndexMVIntDate index, List<TableRowIntDateCols> data, double percent) throws Exception {
        System.out.println(indexName);

        MyTimer myTimer = new MyTimer();

        HashSet<Integer> keys_ = new HashSet<>();
        HashSet<Date> versions_ = new HashSet<>();
        for (TableRowIntDateCols row : data) {
            keys_.add(row.col1);
            versions_.add(row.col2);
        }

        // Sort keys and versions individually? Shouldn't it be sorted together? 
        List<Integer> keys = keys_.stream()
                .sorted(Comparator.comparing(o -> o))
                .collect(Collectors.toList());
        List<Date> versions = versions_.stream()
                .sorted(Comparator.comparing(o -> o))
                .collect(Collectors.toList());

        Date firstVersion = versions.get(0);
        Date currentVersion = firstVersion;

        int keysArrFractionCount = (int) (keys.size()*percent);
        int versionsFractionCount = (int) Math.max(1, versions.size()*percent);

        System.out.println(indexName + " insert-commit start");
        myTimer.start();

        for (TableRowIntDateCols row : data) {
            if (!currentVersion.equals(row.col2)) {
                currentVersion = row.col2;
                index.commitCurrentVersion(currentVersion);
            }
            index.insert(row);
        }

        index.finalizeInsert();

        myTimer.pause();
        double insertLs = myTimer.getElapsedSeconds();
        double insertTPs = ((double) (data.size())) / ((double) insertLs);
        System.out.println(indexName + " insert-commit time (s): " + insertLs);


       List<Integer> keysArrFirstFractionSortedElements = keys.stream().sorted().limit(keysArrFractionCount).collect(Collectors.toList());
        Integer keyStart = keysArrFirstFractionSortedElements.get(0);
        Integer keyEnd = keysArrFirstFractionSortedElements.get(keysArrFirstFractionSortedElements.size() - 1);

        System.out.println(indexName + " rangeSearch1 start");
        ArrayList<IRowDetails<Integer, TableRowIntDateCols,Date>> slr = new ArrayList<>(keys.size());
        myTimer.init();
        myTimer.start();
        index.rangeSearch1(firstVersion, keyStart, keyEnd, slr);
        myTimer.pause();
        double rangeSearch1SkipListLns = myTimer.getElapsedNanoSeconds();
        slr.clear();
        System.out.println(indexName + " rangeSearch1 time (ns): " + rangeSearch1SkipListLns);

        System.out.println(indexName + " rangeSearch1 start");
        ArrayList<Object> mr = new ArrayList<>(keys.size());
        myTimer.init();
        myTimer.start();
        index.rangeSearch1(firstVersion, keyStart, keyEnd, mr);
        myTimer.pause();
        double rangeSearch1MerkleLns = myTimer.getElapsedNanoSeconds();
        mr.clear();
        System.out.println(indexName + " rangeSearch1 time (ns): " + rangeSearch1MerkleLns);


        Date verEnd = versions.get(versionsFractionCount - 1);
        System.out.println(indexName + " rangeSearch3 start");
        myTimer.init();
        myTimer.start();
        index.rangeSearch3(firstVersion, verEnd, keyStart, keyEnd, slr);
        myTimer.pause();
        double rangeSearch3SkipListLms = myTimer.getElapsedMilliSeconds();
        slr.clear();
        System.out.println(indexName + " rangeSearch3 time (ms): " + rangeSearch3SkipListLms);

        System.out.println(indexName + " rangeSearch3 start");
        myTimer.init();
        myTimer.start();
        index.rangeSearch3(firstVersion, verEnd, keyStart, keyEnd, mr);
        myTimer.pause();
        double rangeSearch3MerkleLms = myTimer.getElapsedMilliSeconds();
        mr.clear();
        System.out.println(indexName + " rangeSearch3 time (ms): " + rangeSearch3MerkleLms);


        Integer firstKey = keys.get(0);
        Integer lastKey = keys.get(keys.size()-1);
        System.out.println(indexName + " rangeSearch4 start");
        myTimer.init();
        myTimer.start();
        index.rangeSearch4(firstVersion, verEnd, slr);
        myTimer.pause();
        double rangeSearch4SkipListLms = myTimer.getElapsedMilliSeconds();
        slr.clear();
        System.out.println(indexName + " rangeSearch4 time (ms): " + rangeSearch4SkipListLms);

        System.out.println(indexName + " rangeSearch4 start");
        myTimer.init();
        myTimer.start();
        index.rangeSearch4(firstVersion, verEnd, mr);
        myTimer.pause();
        double rangeSearch4MerkleLms = myTimer.getElapsedMilliSeconds();
        mr.clear();
        System.out.println(indexName + " rangeSearch4 time (ms): " + rangeSearch4MerkleLms);


        Map<String,Object> res = new HashMap<>();
        res.put("count",data.size());
        res.put("versions",versions.size());
        res.put("keysPercent",percent);
        res.put("insertLs",insertLs);
        res.put("insertTPs",insertTPs);
        res.put("rangeSearch1SkipListLns", rangeSearch1SkipListLns);
        res.put("rangeSearch1MerkleLns", rangeSearch1MerkleLns);
        res.put("rangeSearch3SkipListLms", rangeSearch3SkipListLms);
        res.put("rangeSearch3MerkleLms", rangeSearch3MerkleLms);
        res.put("rangeSearch4SkipListLms", rangeSearch4SkipListLms);
        res.put("rangeSearch4MerkleLms", rangeSearch4MerkleLms);

        return res;
    }

    public static Map<String, Object> tableStrIntColsSkipListIndexScenario2(ArrayList<ArrayList<TableRowStrIntCols>> versions, List<String> strArr, double percent) throws Exception {
        System.out.println("tableStrIntColsSkipListIndexScenario2");

        MyTimer myTimer = new MyTimer();
        ToweredTypeUtils<String, TableRowStrIntCols> tableStrIntColsIndexTypeUtils = getTableStrIntColsIndexTypeUtils();
        ToweredSkipList<Integer, String, TableRowStrIntCols> tableStrIntColsIndex = new ToweredSkipList<>(0, iterationProbability, partitionCapacity, tableStrIntColsIndexTypeUtils);

        int intArrFractionCount = (int) (strArr.size()*percent);
        int versionsFractionCount = (int) Math.max(1, versions.size()*percent);


        System.out.println("tableStrIntColsSkipListIndexScenario2 insert-commit start");
        myTimer.start();

        int nextVersion = 1;
        for (ArrayList<TableRowStrIntCols> version : versions) {
            for (TableRowStrIntCols row : version) {
                TableRowStrIntCols tr = new TableRowStrIntCols(row.col1, row.col2, tableStrIntColsIndexTypeUtils.vTypeUtils);
                tableStrIntColsIndex.upsert(tr);
            }
            tableStrIntColsIndex.commitCurrentVersion(nextVersion);
            nextVersion++;
        }

        Utils.assertTrue((nextVersion-1) == versions.size());

        myTimer.pause();
        double insertLs = myTimer.getElapsedSeconds();
        double insertTPs = ((double) (versions.size() * strArr.size())) / ((double) myTimer.getElapsedSeconds());
        System.out.println("tableStrIntColsSkipListIndexScenario2 insert-commit time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("tableStrIntColsSkipListIndexScenario2 update start");
        ArrayList<TableRowStrIntCols> firstVersion = versions.get(0);

        myTimer.init();
        myTimer.start();

//        for (TableRowStrIntCols row : firstVersion) {
//            TableRowStrIntCols tr = new TableRowStrIntCols(row.col1, row.col2, tableStrIntColsIndexTypeUtils.vTypeUtils);
//            tableStrIntColsIndex.upsert(tr);
//        }

        myTimer.pause();
        double updateLs = myTimer.getElapsedSeconds();
        double updateTPs = ((double) firstVersion.size()) / ((double) myTimer.getElapsedSeconds());
        System.out.println("tableStrIntColsSkipListIndexScenario2 update time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("tableStrIntColsSkipListIndexScenario2 rangeSearch1 all start");
        List<String> strArrFirstFractionSortedElements = strArr.stream().sorted().limit(intArrFractionCount).collect(Collectors.toList());
        String keyStart = strArrFirstFractionSortedElements.get(0);
        String keyEnd = strArrFirstFractionSortedElements.get(strArrFirstFractionSortedElements.size() - 1);
        ArrayList<IRowDetails<String, TableRowStrIntCols,Integer>> s = new ArrayList<>(strArr.size());

        myTimer.init();
        myTimer.start();

        tableStrIntColsIndex.rangeSearch1(0, keyStart, keyEnd, s);

        myTimer.pause();
        double rangeSearch1Lms = myTimer.getElapsedMilliSeconds();
        double rangeSearch1TPs = ((double) strArrFirstFractionSortedElements.size()) / ((double) myTimer.getElapsedSeconds());
        System.out.println("tableStrIntColsSkipListIndexScenario2 rangeSearch1 all time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("tableStrIntColsSkipListIndexScenario2 rangeSearch3 all start");
        s.clear();
        int verEnd = versionsFractionCount - 1;

        myTimer.init();
        myTimer.start();

        tableStrIntColsIndex.rangeSearch3(0, verEnd, keyStart, keyEnd, s);

        myTimer.pause();
        double rangeSearch3Lms = myTimer.getElapsedMilliSeconds();
        double rangeSearch3TPs = ((double) (strArrFirstFractionSortedElements.size() * versionsFractionCount)) / ((double) myTimer.getElapsedSeconds());
        System.out.println("tableStrIntColsSkipListIndexScenario2 rangeSearch3 all time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("tableStrIntColsSkipListIndexScenario2 rangeSearch4 all start");
        s.clear();

        myTimer.init();
        myTimer.start();

        tableStrIntColsIndex.rangeSearch3(0, verEnd, "", "ZZZZZZZZZ", s);

        myTimer.pause();
        double rangeSearch4Lms = myTimer.getElapsedMilliSeconds();
        double rangeSearch4TPs = ((double) (versionsFractionCount * strArr.size())) / ((double) myTimer.getElapsedSeconds());
        System.out.println("tableStrIntColsSkipListIndexScenario2 rangeSearch4 all time (ns): " + myTimer.getElapsedNanoSeconds());

        return getStringObjectMapScenario2(versions, strArr, percent, insertLs, insertTPs, updateLs, updateTPs, rangeSearch1Lms, rangeSearch1TPs, rangeSearch3Lms, rangeSearch3TPs, rangeSearch4Lms, rangeSearch4TPs);
    }
    public static Map<String, Object> merkleBucketTreeMVScenario2(int capacity, ArrayList<ArrayList<TableRowStrIntCols>> versions, List<String> strArr, double percent) throws Exception {
        System.out.println("merkleBucketTreeMVScenario");

        MyTimer myTimer = new MyTimer();
        MerkleBucketTreeMVStringInteger mbtMv = new MerkleBucketTreeMVStringInteger(capacity);

        int intArrFractionCount = (int) (strArr.size()*percent);
        int versionsFractionCount = (int) Math.max(1, versions.size()*percent);

        System.out.println("merkleBucketTreeMVScenario insert-commit start");
        myTimer.start();

        int nextVersion = 1;
        for (ArrayList<TableRowStrIntCols> version : versions) {
            for (TableRowStrIntCols row : version) {
                mbtMv.upsert((nextVersion - 1), row.getKey(), row.col2);
            }
            nextVersion++;
        }

        Utils.assertTrue((nextVersion-1) == versions.size());

        myTimer.pause();
        double insertLs = myTimer.getElapsedSeconds();
        double insertTPs = ((double) (versions.size() * strArr.size())) / ((double) myTimer.getElapsedSeconds());
        System.out.println("merkleBucketTreeMVScenario insert-commit time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("merkleBucketTreeMVScenario update start");
        ArrayList<TableRowStrIntCols> firstVersion = versions.get(0);

        myTimer.init();
        myTimer.start();

//        for (TableRowStrIntCols row : firstVersion) {
//            mbtMv.upsert((nextVersion - 1), row.getKey(), row.col2);
//        }

        myTimer.pause();
        double updateLs = myTimer.getElapsedSeconds();
        double updateTPs = ((double) firstVersion.size()) / ((double) myTimer.getElapsedSeconds());
        System.out.println("merkleBucketTreeMVScenario update time (ns): " + myTimer.getElapsedNanoSeconds());


        List<String> strArrFirstFractionSortedElements = strArr.stream().sorted().limit(intArrFractionCount).collect(Collectors.toList());
        String keyStart = strArrFirstFractionSortedElements.get(0);
        String keyEnd = strArrFirstFractionSortedElements.get(strArrFirstFractionSortedElements.size() - 1);

        System.out.println("merkleBucketTreeMVScenario rangeSearch1 all start");
        ArrayList<Integer> s = new ArrayList<>(strArr.size());

        myTimer.init();
        myTimer.start();

        mbtMv.rangeSearch1(0, keyStart, keyEnd, s);

        myTimer.pause();
        double rangeSearch1Lms = myTimer.getElapsedMilliSeconds();
        double rangeSearch1TPs = ((double) strArrFirstFractionSortedElements.size()) / ((double) myTimer.getElapsedSeconds());
        System.out.println("merkleBucketTreeMVScenario rangeSearch1 all time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("merkleBucketTreeMVScenario rangeSearch3 all start");
        s.clear();
        int verEnd = versionsFractionCount - 1;

        myTimer.init();
        myTimer.start();

        mbtMv.rangeSearch3(0, verEnd, keyStart, keyEnd, s);

        myTimer.pause();
        double rangeSearch3Lms = myTimer.getElapsedMilliSeconds();
        double rangeSearch3TPs = ((double) (strArrFirstFractionSortedElements.size() * versionsFractionCount)) / ((double) myTimer.getElapsedSeconds());
        System.out.println("merkleBucketTreeMVScenario rangeSearch3 all time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("merkleBucketTreeMVScenario rangeSearch4 all start");
        s.clear();

        myTimer.init();
        myTimer.start();

        mbtMv.rangeSearch3(0, verEnd, "", "ZZZZZZZZZ", s);

        myTimer.pause();
        double rangeSearch4Lms = myTimer.getElapsedMilliSeconds();
        double rangeSearch4TPs = ((double) (versionsFractionCount * strArr.size())) / ((double) myTimer.getElapsedSeconds());
        System.out.println("merkleBucketTreeMVScenario rangeSearch4 all time (ns): " + myTimer.getElapsedNanoSeconds());

        return getStringObjectMapScenario2(versions, strArr, percent, insertLs, insertTPs, updateLs, updateTPs, rangeSearch1Lms, rangeSearch1TPs, rangeSearch3Lms, rangeSearch3TPs, rangeSearch4Lms, rangeSearch4TPs);
    }

    public static Map<String, Object> patriciaMerkleTrieIndexScenario2(ArrayList<ArrayList<TableRowStrIntCols>> versions, List<String> strArr, double percent) throws Exception {
        System.out.println("patriciaMerkleTrieIndexScenario");

        MyTimer myTimer = new MyTimer();
        PatriciaMerkleTrieMVStringInteger pmtMv = new PatriciaMerkleTrieMVStringInteger();

        int intArrFractionCount = (int) (strArr.size()*percent);
        int versionsFractionCount = (int) Math.max(1, versions.size()*percent);

        System.out.println("patriciaMerkleTrieIndexScenario insert-commit start");
        myTimer.start();

        int nextVersion = 1;
        for (ArrayList<TableRowStrIntCols> version : versions) {
            for (TableRowStrIntCols row : version) {
                pmtMv.upsert((nextVersion - 1), row.getKey(), row.col2);
            }
            nextVersion++;
        }

        Utils.assertTrue((nextVersion-1) == versions.size());

        myTimer.pause();
        double insertLs = myTimer.getElapsedSeconds();
        double insertTPs = ((double) (versions.size() * strArr.size())) / ((double) myTimer.getElapsedSeconds());
        System.out.println("patriciaMerkleTrieIndexScenario insert-commit time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("patriciaMerkleTrieIndexScenario update start");
        ArrayList<TableRowStrIntCols> firstVersion = versions.get(0);

        myTimer.init();
        myTimer.start();

//        for (TableRowStrIntCols row : firstVersion) {
//            pmtMv.upsert((nextVersion - 1), row.getKey(), row.col2);
//        }

        myTimer.pause();
        double updateLs = myTimer.getElapsedSeconds();
        double updateTPs = ((double) firstVersion.size()) / ((double) myTimer.getElapsedSeconds());
        System.out.println("patriciaMerkleTrieIndexScenario update time (ns): " + myTimer.getElapsedNanoSeconds());


        List<String> strArrFirstFractionSortedElements = strArr.stream().sorted().limit(intArrFractionCount).collect(Collectors.toList());
        String keyStart = strArrFirstFractionSortedElements.get(0);
        String keyEnd = strArrFirstFractionSortedElements.get(strArrFirstFractionSortedElements.size() - 1);

        System.out.println("patriciaMerkleTrieIndexScenario rangeSearch1 all start");
        ArrayList<Integer> s = new ArrayList<>(strArr.size());

        myTimer.init();
        myTimer.start();

        pmtMv.rangeSearch1(0, keyStart, keyEnd, s);

        myTimer.pause();
        double rangeSearch1Lms = myTimer.getElapsedMilliSeconds();
        double rangeSearch1TPs = ((double) strArrFirstFractionSortedElements.size()) / ((double) myTimer.getElapsedSeconds());
        System.out.println("patriciaMerkleTrieIndexScenario rangeSearch1 all time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("patriciaMerkleTrieIndexScenario rangeSearch3 all start");
        s.clear();
        int verEnd = versionsFractionCount - 1;

        myTimer.init();
        myTimer.start();

        pmtMv.rangeSearch3(0, verEnd, keyStart, keyEnd, s);

        myTimer.pause();
        double rangeSearch3Lms = myTimer.getElapsedMilliSeconds();
        double rangeSearch3TPs = ((double) (strArrFirstFractionSortedElements.size() * versionsFractionCount)) / ((double) myTimer.getElapsedSeconds());
        System.out.println("patriciaMerkleTrieIndexScenario rangeSearch3 all time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("patriciaMerkleTrieIndexScenario rangeSearch4 all start");
        s.clear();

        myTimer.init();
        myTimer.start();

        pmtMv.rangeSearch3(0, verEnd, "", "ZZZZZZZZZ", s);

        myTimer.pause();
        double rangeSearch4Lms = myTimer.getElapsedMilliSeconds();
        double rangeSearch4TPs = ((double) (versionsFractionCount * strArr.size())) / ((double) myTimer.getElapsedSeconds());
        System.out.println("patriciaMerkleTrieIndexScenario rangeSearch4 all time (ns): " + myTimer.getElapsedNanoSeconds());

        return getStringObjectMapScenario2(versions, strArr, percent, insertLs, insertTPs, updateLs, updateTPs, rangeSearch1Lms, rangeSearch1TPs, rangeSearch3Lms, rangeSearch3TPs, rangeSearch4Lms, rangeSearch4TPs);
    }

    public static Map<String, Object> tableStrIntColsTemporalIndexScenario2(ArrayList<Integer> intArr, ArrayList<String> strArr, double iterationProbability, int partitionCapacity) throws Exception {
        System.out.println("tableStrIntColsTemporalIndex");
        Utils.assertTrue(intArr.size() == strArr.size());

        MyTimer myTimer = new MyTimer();
        IVersionsToKeysIndex<Integer, String> versionsToKeysIndex = new VersionsToConsecutiveKeysIndex<>(0, strArr.size());
        ToweredTypeUtils<String, TableRowStrIntCols> tableStrIntColsIndexTypeUtils = getTableStrIntColsIndexTypeUtils();
        ExtendedMultiVersionSkipList<Integer, String, TableRowStrIntCols> tableStrIntColsIndex = new ExtendedMultiVersionSkipList<>(0, iterationProbability, versionsToKeysIndex, partitionCapacity, tableStrIntColsIndexTypeUtils);
        ITypeUtils<TableRowStrIntCols> tableRowStrIntColsClassUtils = tableStrIntColsIndexTypeUtils.vTypeUtils;

        System.out.println("tableStrIntColsTemporalIndex insert start");
        ArrayList<TableRowStrIntCols> trs = getTableRowStrIntCols(intArr, strArr, tableRowStrIntColsClassUtils);

        myTimer.start();

        for (TableRowStrIntCols tr : trs) {
            tableStrIntColsIndex.insert(tr);
        }

        myTimer.pause();
        double insertLs = myTimer.getElapsedSeconds();
        double insertTPs = ((double) trs.size()) / ((double) myTimer.getElapsedSeconds());
        System.out.println("tableStrIntColsTemporalIndex insert time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("tableStrIntColsTemporalIndex update start");
        trs = getTableRowStrIntCols(intArr, strArr, tableRowStrIntColsClassUtils);

        myTimer.init();
        myTimer.start();

//        for (TableRowStrIntCols tr : trs) {
//            tableStrIntColsIndex.update(tr);
//        }

        myTimer.pause();
        double updateLs = myTimer.getElapsedSeconds();
        double updateTPs = ((double) trs.size()) / ((double) myTimer.getElapsedSeconds());
        System.out.println("tableStrIntColsTemporalIndex update time (ns): " + myTimer.getElapsedNanoSeconds());

        Collections.shuffle(strArr);
        System.out.println("tableStrIntColsTemporalIndex search start");
        myTimer.init();
        myTimer.start();

        for (String key : strArr) {
            TableRowStrIntCols s = tableStrIntColsIndex.search1(0, key);
        }

        myTimer.pause();
        double searchLms = myTimer.getElapsedMilliSeconds();
        double searchTPs = ((double) strArr.size()) / ((double) myTimer.getElapsedSeconds());
        System.out.println("tableStrIntColsTemporalIndex search time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("tableStrIntColsTemporalIndex range all start");
        ArrayList<IRowDetails<String, TableRowStrIntCols,Integer>> s = new ArrayList<>(intArr.size());

        myTimer.init();
        myTimer.start();

        tableStrIntColsIndex.rangeSearch1(0, "", "ZZZZZZZZZ",s);

        myTimer.pause();
        double rangeAllKeysLms = myTimer.getElapsedMilliSeconds();
        double rangeAllKeysTPs = ((double) s.size()) / ((double) myTimer.getElapsedSeconds());
        System.out.println("tableStrIntColsTemporalIndex range all time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("tableStrIntColsTemporalIndex delete start");
        myTimer.init();
        myTimer.start();

//        for (int i = 0; i < count; i++) {
//            String d = strArr.get(i);
//            tableStrIntColsIndex.delete(d);
//        }

        myTimer.pause();
        double deleteLms = myTimer.getElapsedMilliSeconds();
        double deleteTPs = ((double) intArr.size()) / ((double) myTimer.getElapsedSeconds());
        System.out.println("tableStrIntColsTemporalIndex delete time (ns): " + myTimer.getElapsedNanoSeconds());

        return getStringObjectMapScenario2(intArr, insertLs, insertTPs, updateLs, updateTPs, searchLms, searchTPs, rangeAllKeysLms, rangeAllKeysTPs, deleteLms, deleteTPs);
    }

    public static Map<String, Object> tableStrIntColsTemporalIndexScenario2(ArrayList<ArrayList<TableRowStrIntCols>> versions, List<String> strArr, double percent, double iterationProbability) throws Exception {
        System.out.println("tableStrIntColsTemporalIndexScenario");

        MyTimer myTimer = new MyTimer();
        IVersionsToKeysIndex<Integer, String> versionsToKeysIndex = new VersionsToConsecutiveKeysIndex<>(0, strArr.size());
        ToweredTypeUtils<String, TableRowStrIntCols> tableStrIntColsIndexTypeUtils = getTableStrIntColsIndexTypeUtils();
        ExtendedMultiVersionSkipList<Integer, String, TableRowStrIntCols> eMvSl = new ExtendedMultiVersionSkipList<>(0, iterationProbability, versionsToKeysIndex, partitionCapacity, tableStrIntColsIndexTypeUtils);
        ITypeUtils<TableRowStrIntCols> tableRowStrIntColsClassUtils = tableStrIntColsIndexTypeUtils.vTypeUtils;

        int intArrFractionCount = (int) (strArr.size()*percent);
        int versionsFractionCount = (int) Math.max(1, versions.size()*percent);

        System.out.println("tableStrIntColsTemporalIndexScenario insert-commit start");
        myTimer.start();

        int nextVersion = 1;
        for (ArrayList<TableRowStrIntCols> version : versions) {
            for (TableRowStrIntCols row : version) {
                TableRowStrIntCols tr = new TableRowStrIntCols(row.col1, row.col2, tableRowStrIntColsClassUtils);

                if (nextVersion == 1) {
                    eMvSl.insert(tr);
                } else {
                    eMvSl.update(tr);
                }
            }
            eMvSl.commitCurrentVersion(nextVersion);
            nextVersion++;
        }

        Utils.assertTrue((nextVersion-1) == versions.size());

        myTimer.pause();
        double insertLs = myTimer.getElapsedSeconds();
        double insertTPs = ((double) (versions.size() * strArr.size())) / ((double) myTimer.getElapsedSeconds());
        System.out.println("tableStrIntColsTemporalIndexScenario insert-commit time (ns): " + myTimer.getElapsedNanoSeconds());


        System.out.println("tableStrIntColsTemporalIndexScenario update start");
        ArrayList<TableRowStrIntCols> firstVersion = versions.get(0);

        myTimer.init();
        myTimer.start();

//        for (TableRowStrIntCols row : firstVersion) {
//            TableRowStrIntCols tr = new TableRowStrIntCols(row.col1, row.col2, tableRowStrIntColsClassUtils);
//
//            eMvSl.update(tr);
//        }

        myTimer.pause();
        double updateLs = myTimer.getElapsedSeconds();
        double updateTPs = ((double) firstVersion.size()) / ((double) myTimer.getElapsedSeconds());
        System.out.println("tableStrIntColsTemporalIndexScenario update time (ns): " + myTimer.getElapsedNanoSeconds());

        List<String> strArrFirstFractionSortedElements = strArr.stream().sorted().limit(intArrFractionCount).collect(Collectors.toList());
        String keyStart = strArrFirstFractionSortedElements.get(0);
        String keyEnd = strArrFirstFractionSortedElements.get(strArrFirstFractionSortedElements.size() - 1);

        System.out.println("tableStrIntColsTemporalIndexScenario rangeSearch1 all start");
        ArrayList<IRowDetails<String, TableRowStrIntCols,Integer>> s = new ArrayList<>(strArr.size());

        myTimer.init();
        myTimer.start();

        eMvSl.rangeSearch1(0, keyStart, keyEnd, s);

        myTimer.pause();
        double rangeSearch1Lms = myTimer.getElapsedMilliSeconds();
        double rangeSearch1TPs = ((double) strArrFirstFractionSortedElements.size()) / ((double) myTimer.getElapsedSeconds());
        System.out.println("tableStrIntColsTemporalIndexScenario rangeSearch1 all time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("tableStrIntColsTemporalIndexScenario rangeSearch3 all start");
        s.clear();
        int verEnd = versionsFractionCount - 1;

        myTimer.init();
        myTimer.start();

        eMvSl.rangeSearch3(0, verEnd, keyStart, keyEnd, s);

        myTimer.pause();
        double rangeSearch3Lms = myTimer.getElapsedMilliSeconds();
        double rangeSearch3TPs = ((double) (strArrFirstFractionSortedElements.size() * versionsFractionCount)) / ((double) myTimer.getElapsedSeconds());
        System.out.println("tableStrIntColsTemporalIndexScenario rangeSearch3 all time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("tableStrIntColsTemporalIndexScenario rangeSearch4 all start");
        s.clear();

        myTimer.init();
        myTimer.start();

        eMvSl.rangeSearch4(0, verEnd, s);

        myTimer.pause();
        double rangeSearch4Lms = myTimer.getElapsedMilliSeconds();
        double rangeSearch4TPs = ((double) (versionsFractionCount * strArr.size())) / ((double) myTimer.getElapsedSeconds());
        System.out.println("tableStrIntColsTemporalIndexScenario rangeSearch4 all time (ns): " + myTimer.getElapsedNanoSeconds());

        return getStringObjectMapScenario2(versions, strArr, percent, insertLs, insertTPs, updateLs, updateTPs, rangeSearch1Lms, rangeSearch1TPs, rangeSearch3Lms, rangeSearch3TPs, rangeSearch4Lms, rangeSearch4TPs);
    }

    private static Map<String, Object> getStringObjectMapScenario2(ArrayList<Integer> intArr, double insertLs, double insertTPs, double updateLs,
                                                                   double updateTPs, double searchLms, double searchTPs, double rangeAllKeysLms,
                                                                   double rangeAllKeysTPs, double deleteLms, double deleteTPs) {
        Map<String,Object> res = new HashMap<>();
        res.put("count", intArr.size());
        res.put("maxLevel", ToweredSkipList.MAX_LEVEL);
        res.put("insertLs",insertLs);
        res.put("insertTPs",insertTPs);
        res.put("updateLs",updateLs);
        res.put("updateTPs",updateTPs);
        res.put("searchLms",searchLms);
        res.put("searchTPs",searchTPs);
        res.put("rangeAllKeysLms",rangeAllKeysLms);
        res.put("rangeAllKeysTPs",rangeAllKeysTPs);
        res.put("deleteLms",deleteLms);
        res.put("deleteTPs",deleteTPs);

        return res;
    }

    private static Map<String, Object> getStringObjectMapScenario2(ArrayList<ArrayList<TableRowStrIntCols>> versions, List<String> strArr,
                                                                   double percent, double insertLs, double insertTPs, double updateLs, double updateTPs,
                                                                   double rangeSearch1Lms, double rangeSearch1TPs, double rangeSearch3Lms, double rangeSearch3TPs,
                                                                   double rangeSearch4Lms, double rangeSearch4TPs) {
        Map<String,Object> res = new HashMap<>();
        res.put("count",strArr.size());
        res.put("versions",versions.size());
        res.put("keysPercent",percent);
        res.put("insertLs",insertLs);
        res.put("insertTPs",insertTPs);
        res.put("updateLs",updateLs);
        res.put("updateTPs",updateTPs);
        res.put("rangeSearch1Lms", rangeSearch1Lms);
        res.put("rangeSearch1TPs", rangeSearch1TPs);
        res.put("rangeSearch3Lms", rangeSearch3Lms);
        res.put("rangeSearch3TPs", rangeSearch3TPs);
        res.put("rangeSearch4Lms", rangeSearch4Lms);
        res.put("rangeSearch4TPs", rangeSearch4TPs);

        return res;
    }

    private static Map<String, Object> getStringObjectMapScenario3(List<TupleTwo<Integer,Date>> data, List<Date> versions, double percent, double insertLns,
                                                                   double insertTPns, double updateLns, double updateTPns, double rangeSearch1Lns, double rangeSearch1TPns,
                                                                   double rangeSearch3Lns, double rangeSearch3TPns, double rangeSearch4Lns, double rangeSearch4TPns) {
        Map<String,Object> res = new HashMap<>();
        res.put("count",data.size());
        res.put("versions",versions.size());
        res.put("keysPercent",percent);
        res.put("insertLns",insertLns);
        res.put("insertTPns",insertTPns);
        res.put("updateLns",updateLns);
        res.put("updateTPns",updateTPns);
        res.put("rangeSearch1Lns", rangeSearch1Lns);
        res.put("rangeSearch1TPns", rangeSearch1TPns);
        res.put("rangeSearch3Lns", rangeSearch3Lns);
        res.put("rangeSearch3TPns", rangeSearch3TPns);
        res.put("rangeSearch4Lns", rangeSearch4Lns);
        res.put("rangeSearch4TPns", rangeSearch4TPns);

        return res;
    }
    private static void writeScenarioResultsToFile3(Path pathPatriciaMerkleTrieScenario, Map<String, Object> patriciaMerkleTrieRunRes) {
        String line;
        line = String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                patriciaMerkleTrieRunRes.get("count"),
                patriciaMerkleTrieRunRes.get("versions"),
                patriciaMerkleTrieRunRes.get("keysPercent"),
                patriciaMerkleTrieRunRes.get("insertLns"),
                patriciaMerkleTrieRunRes.get("insertTPns"),
                patriciaMerkleTrieRunRes.get("updateLns"),
                patriciaMerkleTrieRunRes.get("updateTPns"),
                patriciaMerkleTrieRunRes.get("rangeSearch1Lns"),
                patriciaMerkleTrieRunRes.get("rangeSearch1TPns"),
                patriciaMerkleTrieRunRes.get("rangeSearch3Lns"),
                patriciaMerkleTrieRunRes.get("rangeSearch3TPns"),
                patriciaMerkleTrieRunRes.get("rangeSearch4Lns"),
                patriciaMerkleTrieRunRes.get("rangeSearch4TPns"));
        Utils.writeToFile(pathPatriciaMerkleTrieScenario, line + System.lineSeparator(), StandardOpenOption.APPEND);
    }

    private static void collectEvaluationResultsScenario3(String logPrefix, String logPostfix, int runs, Double keysPercentsCounts, CsvReader csvReader) throws Exception {
        HashMap<String, HashMap<String, Object>> filesMetaData = new HashMap<>();
        addEntry(merkleBucketTree, "count,versions,keysPercent,insertLns,insertTPns,updateLns,updateTPns,rangeSearch1Lns,rangeSearch1TPns,rangeSearch3Lns,rangeSearch3TPns,rangeSearch4Lns,rangeSearch4TPns", filesMetaData);
        addEntry(patriciaMerkleTrie, "count,versions,keysPercent,insertLns,insertTPns,updateLns,updateTPns,rangeSearch1Lns,rangeSearch1TPns,rangeSearch3Lns,rangeSearch3TPns,rangeSearch4Lns,rangeSearch4TPns", filesMetaData);
        addEntry(temporalSkipList, "count,versions,keysPercent,insertLns,insertTPns,updateLns,updateTPns,rangeSearch1Lns,rangeSearch1TPns,rangeSearch3Lns,rangeSearch3TPns,rangeSearch4Lns,rangeSearch4TPns", filesMetaData);
        addEntry(temporalIndex, "count,versions,keysPercent,insertLns,insertTPns,updateLns,updateTPns,rangeSearch1Lns,rangeSearch1TPns,rangeSearch3Lns,rangeSearch3TPns,rangeSearch4Lns,rangeSearch4TPns", filesMetaData);

        HashMap<String, Object> settings = new HashMap<>();

        settings.put("runsCount", runs);
        settings.put("MBTCapacity", 100000);
        settings.put("temporalIndexCapacity", 20);
        settings.put("percent", keysPercentsCounts);


        collectEvaluationResultsScenario3(logPrefix, "Scenario3" + logPostfix, settings, filesMetaData, csvReader, true);

        ProcessRunsAvg(Paths.get(logPrefix + merkleBucketTree + "Scenario3" + logPostfix),
                Paths.get(logPrefix + merkleBucketTree + "Scenario3" + runsAvgProcessed + logPostfix),
                filesMetaData.get(merkleBucketTree), new HashSet<>(Arrays.asList(0, 1, 2)));
        ProcessRunsAvg(Paths.get(logPrefix + patriciaMerkleTrie + "Scenario3" + logPostfix),
                Paths.get(logPrefix + patriciaMerkleTrie + "Scenario3" + runsAvgProcessed + logPostfix),
                filesMetaData.get(patriciaMerkleTrie), new HashSet<>(Arrays.asList(0, 1, 2)));
        ProcessRunsAvg(Paths.get(logPrefix + temporalSkipList + "Scenario3" + logPostfix),
                Paths.get(logPrefix + temporalSkipList + "Scenario3" + runsAvgProcessed + logPostfix),
                filesMetaData.get(temporalSkipList), new HashSet<>(Arrays.asList(0, 1, 2)));
        ProcessRunsAvg(Paths.get(logPrefix + temporalIndex + "Scenario3" + logPostfix),
                Paths.get(logPrefix + temporalIndex + "Scenario3" + runsAvgProcessed + logPostfix),
                filesMetaData.get(temporalIndex), new HashSet<>(Arrays.asList(0, 1, 2)));
    }

    public static void collectEvaluationResultsScenario3(String logPrefix, String logPostfix, HashMap<String, Object> settings, HashMap<String,HashMap<String,Object>> filesMetaData, CsvReader csvReader, boolean initCsvFiles) throws Exception {


        Path pathMerkleBucketScenario = Paths.get(logPrefix + merkleBucketTree + logPostfix);
        Path pathPatriciaMerkleTrieScenario = Paths.get(logPrefix + patriciaMerkleTrie + logPostfix);
        Path pathTempSkipListScenario = Paths.get(logPrefix + temporalSkipList + logPostfix);
        Path pathTempIndexScenario = Paths.get(logPrefix + temporalIndex + logPostfix);

        if (initCsvFiles) {
            initFile(merkleBucketTree, filesMetaData, pathMerkleBucketScenario);
            initFile(patriciaMerkleTrie, filesMetaData, pathPatriciaMerkleTrieScenario);
            initFile(temporalSkipList, filesMetaData, pathTempSkipListScenario);
            initFile(temporalIndex, filesMetaData, pathTempIndexScenario);
        }

        int runsCount = (int) settings.get("runsCount");
        int MBTCapacity = (int) settings.get("MBTCapacity");
        ToweredSkipList.MAX_LEVEL = (int) settings.get("temporalIndexCapacity");
        double percent = (double) settings.get("percent");


//        ArrayList<TupleTwo<Integer, Date>> data = getMedicalDataSet(csvReader);

        int totalRowsCount = 1000000;
        LocalDate startLocalDate = LocalDate.of(2015, Month.JANUARY, 1);
        int datesCount = 365;
        int firstPatientID = 1;
        double patientsPerDateRatio = 0.8;

        for (int runs = 0; runs < runsCount; runs++) {

            ArrayList<TupleTwo<Integer, Date>> data = getSyntheticMedicalDataSet(totalRowsCount, startLocalDate, datesCount, firstPatientID, patientsPerDateRatio);

            Map<String, Object> skipListIndexRunRes = tableIntDateColsSkipListIndexScenario3(data, percent);
            writeScenarioResultsToFile3(pathTempSkipListScenario, skipListIndexRunRes);

            Map<String, Object> patriciaMerkleTrieRunRes = patriciaMerkleTrieMVIntDateIndexScenario3(data, percent);
            writeScenarioResultsToFile3(pathPatriciaMerkleTrieScenario, patriciaMerkleTrieRunRes);
//
            Map<String, Object> merkleBucketIndexRunRes = merkleBucketTreeMVIntDateScenario3(data, MBTCapacity, percent);
            writeScenarioResultsToFile3(pathMerkleBucketScenario, merkleBucketIndexRunRes);
//
            Map<String, Object> tempIndexRunRes = tableIntDateColsTemporalIndexScenario3(data, percent, iterationProbability);
            writeScenarioResultsToFile3(pathTempIndexScenario, tempIndexRunRes);

        }


    }

    private static ArrayList<TupleTwo<Integer, Date>> getSyntheticMedicalDataSet(int totalRowsCount, LocalDate startLocalDate, int datesCount, int firstPatientID, double patientsPerDateRatio) {

        int patientIDsSize = (int) ((totalRowsCount/datesCount)/patientsPerDateRatio);
        int patientIDsPerDayCount = (int) (patientIDsSize*patientsPerDateRatio);

        ArrayList<TupleTwo<Integer, Date>> res = new ArrayList<>();

        LocalDate currentLocalDate = startLocalDate;
        ZoneId defaultZoneId = ZoneId.systemDefault();
        ArrayList<Date> sequentialDates = new ArrayList<>(); // Dates starting from 2015 until datesCount goes to zero

        while (datesCount > 0) {
            Date currentDate = Date.from(currentLocalDate.atStartOfDay(defaultZoneId).toInstant());
            sequentialDates.add(currentDate);
            currentLocalDate = currentLocalDate.plusDays(1);
            datesCount--;
        }

        ArrayList<Integer> patientIDs = generateSortedNumbers(firstPatientID, 1, patientIDsSize);

        for (Date date : sequentialDates) {
            Collections.shuffle(patientIDs);
            for (int i=0; i<patientIDsPerDayCount; i++) {
                res.add(new TupleTwo<>(patientIDs.get(i), date));
            }
        }

        // res = List<patientID, date> 

        return res;
    }

    private static ArrayList<TupleTwo<Integer, Date>> getMedicalDataSet(CsvReader csvReader) {
        ArrayList<TupleTwo<Integer, Date>> data = new ArrayList<>();
        for (Map<String, String> row : csvReader.data) {
            String participantIDStr = row.get("participant");
            Integer participantIDInt = Integer.parseInt(participantIDStr);
            String dateStr = row.get("date");
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDate localDate = LocalDate.parse(dateStr, formatter);
//            double dateMilli = date.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();
            ZoneId defaultZoneId = ZoneId.systemDefault();
            Date date = Date.from(localDate.atStartOfDay(defaultZoneId).toInstant());

            TupleTwo<Integer, Date> t = new TupleTwo<>(participantIDInt, date);
            data.add(t);
        }
        return data;
    }

    public static Map<String, Object> tableIntDateColsSkipListIndexScenario3(List<TupleTwo<Integer, Date>> data, double percent) throws Exception {
        System.out.println("tableIntDateColsSkipListIndexScenario3");

        MyTimer myTimer = new MyTimer();
        ToweredTypeUtils<Integer, TableRowIntDateCols> tableIntDateColsIndexTypeUtils = getTableIntDateColsIndexTypeUtils();

        HashSet<Integer> keys_ = new HashSet<>();
        HashSet<Date> versions_ = new HashSet<>();
        for (TupleTwo<Integer, Date> row : data) {
            keys_.add(row.first);
            versions_.add(row.second);
        }

        List<Integer> keys = keys_.stream()
                .sorted(Comparator.comparing(o -> o))
                .collect(Collectors.toList());
        List<Date> versions = versions_.stream()
                .sorted(Comparator.comparing(o -> o))
                .collect(Collectors.toList());

        Date firstVersion = versions.get(0);
        Date currentVersion = firstVersion;

        int keysArrFractionCount = (int) (keys.size()*percent);
        int versionsFractionCount = (int) Math.max(1, versions.size()*percent);

        ToweredSkipList<Date, Integer, TableRowIntDateCols> tableIntDateColsIndex = new ToweredSkipList<>(currentVersion, iterationProbability, partitionCapacity, tableIntDateColsIndexTypeUtils);

        System.out.println("tableIntDateColsSkipListIndexScenario3 insert-commit start");
        myTimer.start();

        for (TupleTwo<Integer, Date> row : data) {
            if (!currentVersion.equals(row.second)) {
                currentVersion = row.second;
                tableIntDateColsIndex.commitCurrentVersion(currentVersion);
            }

            TableRowIntDateCols tr = new TableRowIntDateCols(row.first, row.second, tableIntDateColsIndexTypeUtils.vTypeUtils);
            tableIntDateColsIndex.upsert(tr);
        }

        myTimer.pause();
        double insertLns = myTimer.getElapsedNanoSeconds();
        System.out.println("tableIntDateColsSkipListIndexScenario3 insert-commit time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("tableIntDateColsSkipListIndexScenario3 update start");
        myTimer.init();
        myTimer.start();

//        for (TableRowStrIntCols row : firstVersion) {
//            TableRowStrIntCols tr = new TableRowStrIntCols(row.col1, row.col2, tableStrIntColsIndexTypeUtils.vTypeUtils);
//            tableIntDateColsIndex.upsert(tr);
//        }

        myTimer.pause();
        double updateLns = myTimer.getElapsedNanoSeconds();
        System.out.println("tableIntDateColsSkipListIndexScenario3 update time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("tableIntDateColsSkipListIndexScenario3 rangeSearch1 all start");
        List<Integer> keysArrFirstFractionSortedElements = keys.stream().sorted().limit(keysArrFractionCount).collect(Collectors.toList());
        Integer keyStart = keysArrFirstFractionSortedElements.get(0);
        Integer keyEnd = keysArrFirstFractionSortedElements.get(keysArrFirstFractionSortedElements.size() - 1);
        ArrayList<IRowDetails<Integer, TableRowIntDateCols,Date>> s = new ArrayList<>(keys.size());

        myTimer.init();
        myTimer.start();

        tableIntDateColsIndex.rangeSearch1(firstVersion, keyStart, keyEnd, s);

        myTimer.pause();
        double rangeSearch1Lns = myTimer.getElapsedNanoSeconds();

        System.out.println("tableIntDateColsSkipListIndexScenario3 rangeSearch1 all time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("tableIntDateColsSkipListIndexScenario3 rangeSearch3 all start");
        s.clear();
        Date verEnd = versions.get(versionsFractionCount - 1);

        myTimer.init();
        myTimer.start();

        tableIntDateColsIndex.rangeSearch3(firstVersion, verEnd, keyStart, keyEnd, s);

        myTimer.pause();
        double rangeSearch3Lns = myTimer.getElapsedNanoSeconds();
        System.out.println("tableIntDateColsSkipListIndexScenario3 rangeSearch3 all time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("tableIntDateColsSkipListIndexScenario3 rangeSearch4 all start");
        s.clear();

        Integer firstKey = keys.get(0);
        Integer lastKey = keys.get(keys.size()-1);

        myTimer.init();
        myTimer.start();

        tableIntDateColsIndex.rangeSearch3(firstVersion, verEnd, firstKey, lastKey, s);

        myTimer.pause();
        double rangeSearch4Lns = myTimer.getElapsedNanoSeconds();
        System.out.println("tableIntDateColsSkipListIndexScenario3 rangeSearch4 all time (ns): " + myTimer.getElapsedNanoSeconds());

        return getStringObjectMapScenario3(data, versions, percent, insertLns, 0, updateLns, 0, rangeSearch1Lns, 0, rangeSearch3Lns, 0, rangeSearch4Lns, 0);
    }

    public static Map<String, Object> patriciaMerkleTrieMVIntDateIndexScenario3(List<TupleTwo<Integer,Date>> data, double percent) throws Exception {
        System.out.println("patriciaMerkleTrieMVIntDateIndexScenario3");

        MyTimer myTimer = new MyTimer();


        HashSet<Integer> keys_ = new HashSet<>();
        HashSet<Date> versions_ = new HashSet<>();
        for (TupleTwo<Integer, Date> row : data) {
            keys_.add(row.first);
            versions_.add(row.second);
        }

        List<Integer> keys = new ArrayList<>(keys_);
        List<Date> versions = new ArrayList<>(versions_);

        keys = keys.stream()
                .sorted(Comparator.comparing(o -> o))
                .collect(Collectors.toList());
        versions = versions.stream()
                .sorted(Comparator.comparing(o -> o))
                .collect(Collectors.toList());


        Date firstVersion = versions.get(0);
        PatriciaMerkleTrieMVIntDate pmtMv = new PatriciaMerkleTrieMVIntDate();



        int keysArrFractionCount = (int) (keys.size()*percent);
        int versionsFractionCount = (int) Math.max(1, versions.size()*percent);


        System.out.println("patriciaMerkleTrieMVIntDateIndexScenario3 insert-commit start");
        myTimer.start();


        for (TupleTwo<Integer, Date> row : data) {
//            CompositeKey<Integer, Date> ck = new CompositeKey<>(row.first, row.second);
//            pmtMv.upsert(ck, row.first);
            pmtMv.upsert(row.second, row.first, row.first);
        }

        myTimer.pause();
        double insertLns = myTimer.getElapsedNanoSeconds();

        System.out.println("patriciaMerkleTrieMVIntDateIndexScenario3 insert-commit time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("patriciaMerkleTrieMVIntDateIndexScenario3 update start");


        myTimer.init();
        myTimer.start();

//        for (TableRowStrIntCols row : firstVersion) {
//            TableRowStrIntCols tr = new TableRowStrIntCols(row.col1, row.col2, tableStrIntColsIndexTypeUtils.vTypeUtils);
//            pmtMv.upsert(tr);
//        }

        myTimer.pause();
        double updateLns = myTimer.getElapsedNanoSeconds();

        System.out.println("patriciaMerkleTrieMVIntDateIndexScenario3 update time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("patriciaMerkleTrieMVIntDateIndexScenario3 rangeSearch1 all start");
        List<Integer> keysArrFirstFractionSortedElements = keys.stream().sorted().limit(keysArrFractionCount).collect(Collectors.toList());
        Integer keyStart = keysArrFirstFractionSortedElements.get(0);
        Integer keyEnd = keysArrFirstFractionSortedElements.get(keysArrFirstFractionSortedElements.size() - 1);
        ArrayList<Object> s = new ArrayList<>(keys.size());

        myTimer.init();
        myTimer.start();

        pmtMv.rangeSearch1(firstVersion, keyStart, keyEnd, s);

        myTimer.pause();
        double rangeSearch1Lns = myTimer.getElapsedNanoSeconds();

        System.out.println("patriciaMerkleTrieMVIntDateIndexScenario3 rangeSearch1 all time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("patriciaMerkleTrieMVIntDateIndexScenario3 rangeSearch3 all start");
        s.clear();
        Date verEnd = versions.get(versionsFractionCount - 1);

        myTimer.init();
        myTimer.start();

        pmtMv.rangeSearch3(firstVersion, verEnd, keyStart, keyEnd, s);

        myTimer.pause();
        double rangeSearch3Lns = myTimer.getElapsedNanoSeconds();
        System.out.println("patriciaMerkleTrieMVIntDateIndexScenario3 rangeSearch3 all time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("patriciaMerkleTrieMVIntDateIndexScenario3 rangeSearch4 all start");
        s.clear();

        Integer firstKey = keys.get(0);
        Integer lastKey = keys.get(keys.size()-1);

        myTimer.init();
        myTimer.start();

        pmtMv.rangeSearch3(firstVersion, verEnd, firstKey, lastKey, s);

        myTimer.pause();
        double rangeSearch4Lns = myTimer.getElapsedNanoSeconds();
        System.out.println("patriciaMerkleTrieMVIntDateIndexScenario3 rangeSearch4 all time (ns): " + myTimer.getElapsedNanoSeconds());

        return getStringObjectMapScenario3(data, versions, percent, insertLns, 0, updateLns, 0, rangeSearch1Lns, 0, rangeSearch3Lns, 0, rangeSearch4Lns, 0);
    }

    public static Map<String, Object> tableIntDateColsTemporalIndexScenario3(List<TupleTwo<Integer, Date>> data, double percent, double iterationProbability) throws Exception {



        System.out.println("tableIntDateColsTemporalIndexScenario3");

        MyTimer myTimer = new MyTimer();
        ToweredTypeUtils<Integer, TableRowIntDateCols> tableIntDateColsIndexTypeUtils = getTableIntDateColsIndexTypeUtils();

        HashSet<Integer> keys_ = new HashSet<>();
        HashSet<Date> versions_ = new HashSet<>();
        for (TupleTwo<Integer, Date> row : data) {
            keys_.add(row.first);
            versions_.add(row.second);
        }

        List<Integer> keys = keys_.stream()
                .sorted(Comparator.comparing(o -> o))
                .collect(Collectors.toList());
        List<Date> versions = versions_.stream()
                .sorted(Comparator.comparing(o -> o))
                .collect(Collectors.toList());

        Date firstVersion = versions.get(0);
        Date currentVersion = firstVersion;

        int keysArrFractionCount = (int) (keys.size()*percent);
        int versionsFractionCount = (int) Math.max(1, versions.size()*percent);

        IVersionsToKeysIndex<Date, Integer> versionsToKeysIndex = new VersionsToKeysIndex<>(currentVersion, versions.size());
        ExtendedMultiVersionSkipList<Date, Integer, TableRowIntDateCols> eMvSl = new ExtendedMultiVersionSkipList<>(firstVersion, iterationProbability, versionsToKeysIndex, partitionCapacity, tableIntDateColsIndexTypeUtils);

        System.out.println("tableIntDateColsTemporalIndexScenario3 insert-commit start");
        myTimer.start();

        for (TupleTwo<Integer, Date> row : data) {
            if (!currentVersion.equals(row.second)) {
                currentVersion = row.second;
                eMvSl.commitCurrentVersion(currentVersion);
            }

            TableRowIntDateCols tr = new TableRowIntDateCols(row.first, row.second, tableIntDateColsIndexTypeUtils.vTypeUtils);
            eMvSl.insert(tr);
        }

        myTimer.pause();
        double insertLns = myTimer.getElapsedNanoSeconds();
        System.out.println("tableIntDateColsTemporalIndexScenario3 insert-commit time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("tableIntDateColsTemporalIndexScenario3 update start");
        myTimer.init();
        myTimer.start();

//        for (TableRowStrIntCols row : firstVersion) {
//            TableRowStrIntCols tr = new TableRowStrIntCols(row.col1, row.col2, tableIntDateColsIndexTypeUtils.vTypeUtils);
//            eMvSl.upsert(tr);
//        }

        myTimer.pause();
        double updateLns = myTimer.getElapsedNanoSeconds();
        System.out.println("tableIntDateColsTemporalIndexScenario3 update time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("tableIntDateColsTemporalIndexScenario3 rangeSearch1 all start");
        List<Integer> keysArrFirstFractionSortedElements = keys.stream().sorted().limit(keysArrFractionCount).collect(Collectors.toList());
        Integer keyStart = keysArrFirstFractionSortedElements.get(0);
        Integer keyEnd = keysArrFirstFractionSortedElements.get(keysArrFirstFractionSortedElements.size() - 1);
        ArrayList<IRowDetails<Integer, TableRowIntDateCols,Date>> s = new ArrayList<>(keys.size());

        myTimer.init();
        myTimer.start();

        eMvSl.rangeSearch1(firstVersion, keyStart, keyEnd, s);

        myTimer.pause();
        double rangeSearch1Lns = myTimer.getElapsedNanoSeconds();

        System.out.println("tableIntDateColsTemporalIndexScenario3 rangeSearch1 all time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("tableIntDateColsTemporalIndexScenario3 rangeSearch3 all start");
        s.clear();
        Date verEnd = versions.get(versionsFractionCount - 1);

        myTimer.init();
        myTimer.start();

        eMvSl.rangeSearch3(firstVersion, verEnd, keyStart, keyEnd, s);

        myTimer.pause();
        double rangeSearch3Lns = myTimer.getElapsedNanoSeconds();
        System.out.println("tableIntDateColsTemporalIndexScenario3 rangeSearch3 all time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("tableIntDateColsTemporalIndexScenario3 rangeSearch4 all start");
        s.clear();

        Integer firstKey = keys.get(0);
        Integer lastKey = keys.get(keys.size()-1);

        myTimer.init();
        myTimer.start();

        eMvSl.rangeSearch3(firstVersion, verEnd, firstKey, lastKey, s);

        myTimer.pause();
        double rangeSearch4Lns = myTimer.getElapsedNanoSeconds();
        System.out.println("tableIntDateColsTemporalIndexScenario3 rangeSearch4 all time (ns): " + myTimer.getElapsedNanoSeconds());

        return getStringObjectMapScenario3(data, versions, percent, insertLns, 0, updateLns, 0, rangeSearch1Lns, 0, rangeSearch3Lns, 0, rangeSearch4Lns, 0);
    }

    public static Map<String, Object> merkleBucketTreeMVIntDateScenario3(List<TupleTwo<Integer, Date>> data, int capacity, double percent) throws Exception {
        System.out.println("merkleBucketTreeMVIntDateScenario3");

        MyTimer myTimer = new MyTimer();


        HashSet<Integer> keys_ = new HashSet<>();
        HashSet<Date> versions_ = new HashSet<>();
        for (TupleTwo<Integer, Date> row : data) {
            keys_.add(row.first);
            versions_.add(row.second);
        }

        List<Integer> keys = new ArrayList<>(keys_);
        List<Date> versions = new ArrayList<>(versions_);

        keys = keys.stream()
                .sorted(Comparator.comparing(o -> o))
                .collect(Collectors.toList());
        versions = versions.stream()
                .sorted(Comparator.comparing(o -> o))
                .collect(Collectors.toList());


        Date firstVersion = versions.get(0);
        MerkleBucketTreeMVIntDate mbtMv = new MerkleBucketTreeMVIntDate(capacity);
//        MerkleBucketTreeMVStringInteger mbtMv = new MerkleBucketTreeMVStringInteger(capacity);


        int keysArrFractionCount = (int) (keys.size()*percent);
        int versionsFractionCount = (int) Math.max(1, versions.size()*percent);


        System.out.println("merkleBucketTreeMVIntDateScenario3 insert-commit start");
        myTimer.start();


        for (TupleTwo<Integer, Date> row : data) {
            mbtMv.upsert(row.second, row.first, row.first);
        }

        myTimer.pause();
        double insertLns = myTimer.getElapsedNanoSeconds();

        System.out.println("merkleBucketTreeMVIntDateScenario3 insert-commit time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("merkleBucketTreeMVIntDateScenario3 update start");


        myTimer.init();
        myTimer.start();

//        for (TableRowStrIntCols row : firstVersion) {
//            TableRowStrIntCols tr = new TableRowStrIntCols(row.col1, row.col2, tableStrIntColsIndexTypeUtils.vTypeUtils);
//            mbtMv.upsert(tr);
//        }

        myTimer.pause();
        double updateLns = myTimer.getElapsedNanoSeconds();

        System.out.println("merkleBucketTreeMVIntDateScenario3 update time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("merkleBucketTreeMVIntDateScenario3 rangeSearch1 all start");
        List<Integer> keysArrFirstFractionSortedElements = keys.stream().sorted().limit(keysArrFractionCount).collect(Collectors.toList());
        Integer keyStart = keysArrFirstFractionSortedElements.get(0);
        Integer keyEnd = keysArrFirstFractionSortedElements.get(keysArrFirstFractionSortedElements.size() - 1);
        ArrayList<Object> s = new ArrayList<>(keys.size());

        myTimer.init();
        myTimer.start();

        mbtMv.rangeSearch1(firstVersion, keyStart, keyEnd, s);

        myTimer.pause();
        double rangeSearch1Lns = myTimer.getElapsedNanoSeconds();

        System.out.println("merkleBucketTreeMVIntDateScenario3 rangeSearch1 all time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("merkleBucketTreeMVIntDateScenario3 rangeSearch3 all start");
        s.clear();
        Date verEnd = versions.get(versionsFractionCount - 1);

        myTimer.init();
        myTimer.start();

        mbtMv.rangeSearch3(firstVersion, verEnd, keyStart, keyEnd, s);

        myTimer.pause();
        double rangeSearch3Lns = myTimer.getElapsedNanoSeconds();
        System.out.println("merkleBucketTreeMVIntDateScenario3 rangeSearch3 all time (ns): " + myTimer.getElapsedNanoSeconds());

        System.out.println("merkleBucketTreeMVIntDateScenario3 rangeSearch4 all start");
        s.clear();

        Integer firstKey = keys.get(0);
        Integer lastKey = keys.get(keys.size()-1);

        myTimer.init();
        myTimer.start();

        mbtMv.rangeSearch3(firstVersion, verEnd, firstKey, lastKey, s);

        myTimer.pause();
        double rangeSearch4Lns = myTimer.getElapsedNanoSeconds();
        System.out.println("merkleBucketTreeMVIntDateScenario3 rangeSearch4 all time (ns): " + myTimer.getElapsedNanoSeconds());

        return getStringObjectMapScenario3(data, versions, percent, insertLns, 0, updateLns, 0, rangeSearch1Lns, 0, rangeSearch3Lns, 0, rangeSearch4Lns, 0);
    }
}
