package youtuberate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.test.PathUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class TestYouTubeStat {

    private static final String CLUSTER_1 = "cluster1";

    private File testDataPath;
    private Configuration conf;
    private MiniDFSCluster cluster;
    private FileSystem fs;

    @Before
    public void setUp() throws Exception {
        testDataPath = new File(PathUtils.getTestDir(getClass()),
                "miniclusters");

        System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA);
        conf = new HdfsConfiguration();

        File testDataCluster1 = new File(testDataPath, CLUSTER_1);
        String c1Path = testDataCluster1.getAbsolutePath();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, c1Path);
        cluster = new MiniDFSCluster.Builder(conf).build();

        fs = FileSystem.get(conf);
    }

    @After
    public void tearDown() throws Exception {
        Path dataDir = new Path(
                testDataPath.getParentFile().getParentFile().getParent());
        fs.delete(dataDir, true);
        File rootTestFile = new File(testDataPath.getParentFile().getParentFile().getParent());
        String rootTestDir = rootTestFile.getAbsolutePath();
        Path rootTestPath = new Path(rootTestDir);
        LocalFileSystem localFileSystem = FileSystem.getLocal(conf);
        localFileSystem.delete(rootTestPath, true);
        cluster.shutdown();
    }

    @Test
    public void testClusterWithData() throws Throwable {

        String IN_DIR = "testing/youtubestat/input";
        String OUT_DIR = "testing/youtubestat/output";
        String DATA_FILE = "sample.txt";

        Path inDir = new Path(IN_DIR);
        Path outDir = new Path(OUT_DIR);

        fs.delete(inDir, true);
        fs.delete(outDir,true);

        // create the input data files
        List<String> content = new ArrayList<String>();
        content.add("UakKQp_ZCuo\tsmolesiuk\t1209\tPeople & Blogs\t1371\t13791\t4.95\t270\t739\tGrFMWUeEUgU\t0ZzYc0odKf0\tSh0nmCYTurs\tZAtg5EfPLVU\ttQcoRN0-YZA\tGkjPd9Pl778\taODZt0bewJo\tB-KjVZ7Yp8k\t68hRs2KyQns\tZtT57plaGPU\tHgsiAUBsFJI\t3wP9eMKe64g\t0euPNqvFM18\tHgrPHYcF5lI\tARV2hP4PiPM\tN0SzcLXpMcQ\tx3bxwTKgxVk\tIyulipTFqd8\t1n47XZE3sAI\ttNFMqWhD95U");
        content.add("lreQ8uhDGxQ\txteeener\t1209\tHowto & Style\t239\t2585\t4.95\t62\t529\tHbpRav8bjjQ\tkcf6Ak16Aeo\t#NAME?\tGyJUkmTiZYI\tVs6TV2T7guY\t6L6JmWjD9mc\tgrbr4xXuQog\tUJEx63_sQWA\tvx27gqmec5g\tpiBuHL8VIHA\tgMTA0vq_WDw\tH_Ak1Tg2BOc\tOG9lY26l5M4\tkdbOo18Ym14\tet6yUNr4pcQ\tRdCLfiJcFuQ\tHgdG6aECFBU\tAuHdCyArxbw\tukl6E2RjpbM\t-17Zbw0e6zs");
        content.add("H484Zj344rU\tmYcheMicALroMaNcE\t1210\tMusic\t310\t11078\t4.95\t578\t571\tEspK_8TbKRc\tXF_Fqddu7kw\t5mnWcstsmqU\tkVNBhxV2eQ0\tf6p37nOp2v8\tIQj7vOQmosQ\tPxBzdmhT99o\tzzqoEk7Tm7A\t7NN-vQvucNU\tOwbskTIo9Ng\tMweZV2Djna4\tJ9_U5k1xwR4\tb8aa71p93_0\tHfzL0Sq2Mvc\tpDk6VUwW7mk\tdjxc-bQPLwA\tv-OlTMBidFs\tPUj1ExZszy4\t8rTUaICON6k\tvX06zsrDfNE");
        content.add("RA5uJIz4y0A\tFLuffeeTalks\t1209\tComedy\t98\t11795\t4.95\t697\t530\tG5FcQKlr5zQ\txXRQ59ri4kE\tHK0_O7O80I8\tZDvz2NAvbFk\tQQ-HwRG2JLs\tepXcjcj0mW4\tA2XPiqhN_Ns\t_5bMfkOHzyQ\tS4c_oJCulgg\tEM3Re7JX_4o\t_E4PYoAqSmg\thTV8Hm3A68Q\tAA5ZR_jxdXU\tWfHGX2pVg7c\tuNl-ongb5YA\tTq0tLHhsWtQ\t4c3egjro1JU\tXL1XD5fAcB4\t1idp7b9SP0E\tx3OVNVaFpLU");
        content.add("Eu3qxeZmBrk\tmachinima\t1209\tEntertainment\t779\t223395\t4.95\t3855\t2432\t#NAME?\tY7szPXlQuNo\tt8I-KW7ZYWY\taZBDzdYbbys\tBxpCfy2qv6E\tqjfEJusOXYU\taDR25D5DndQ\t91tGuLN8c24\tZaF1SdnE0LI\trtmXRBQBQFo\ts9qH2pZJqjs\tBS2MGgtb7lk\tCVGg6wwes-c\tNl3F7QyykAM\tIQAozgj6g1Y\t4FV61o7S8QI\tUKcC2dE-x_E\tss-d-E6J-uE\tfs8G9OBtplM\tHCaElWpyLCQ");
        content.add("Pz69RtizwxA\tcommunitychannel\t1209\tPeople & Blogs\t213\t52851\t4.95\t2471\t829\tixiTYBlVjo4\tZGLJ0BZARI4\txZ8yaLK0XxU\tI3eyzyI9Urs\tRjVKz_OLkzo\takTAuK_qRW0\tixiTYBlVjo4\tDxrKHJCwTgs\tXYCKTe3QPic\tyP7S7IC8u8A\t_sZjUHDO4A0\tfBwc30gxkBI\t#NAME?\tCc9FrcOkOTc\ttazv96BCcPw\t1BYZjgnnSSI\t8mebL0Q4KVs\tBnQHjwSHIhg\tL9OxvhObaWo\tXhoOo9IEeTs");
        content.add("QFXMq-4aCMA\tSamiKhedira\t1209\tSports\t152\t42413\t4.95\t578\t485\tj1jRdnylnwI\tXTVGFyTo3ms\tV2YbGo4ifpQ\tLeSDRTnCnPI\tJBVYLhQaCDc\tTfxEtf45xVY\tJSTjeXj3ZwI\t91b-XGbJnAs\teWbPD5yzbNA\tx4rHQhzmqac\tejEVczA8PLU\tJGCMpnP9tV4\twuik2Ce7tkg\tVaDyI76gDPA\tRAvNQyjvhYc\t8afMQ0_Aqq8\tRWuEcKZ5knU\tGRG9QxlOtQo\t1ai5QLk6tLk\tdSijRmZMMiY");
        content.add("3uAF_b7Rnq0\tNBA\t1210\tSports\t200\t82098\t4.71\t259\t681\t_oQfMYu3fAk\tc1gPDd26aPk\tH_z7osy03LY\tv8kekiBsW_k\t1RBojci9mOg\tepTg03596kU\tO1e8smnOwuY\tKjxTg_R5zxY\tLwDa3dK3g28\tFf_zWO2TxuY\tPCfnK0FqKnc\tYB-9hVvmyYw\t3ds9sB_K4VY\tDLxu37c1Tmk\tO2Nz730To-U\tZEUa_0q72vQ\tChoRgFwS-8w\tZHtnno9dZ_U\trj326z-4mxk\tHvBh6azN-7c");

        writeHDFSContent(fs, inDir, DATA_FILE, content);

        Job job = Job.getInstance(conf, "Test Youtube Statistics");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(YouTubeStat.VideoStatMapper.class);
        job.setReducerClass(YouTubeStat.ArrayMultiOutputReducer.class);
        FileInputFormat.addInputPath(job, inDir);
        FileOutputFormat.setOutputPath(job, outDir);
        job.waitForCompletion(true);
        assertEquals(JobStatus.State.SUCCEEDED, job.getJobState());

        // check output files
        FileStatus[] files = fs.listStatus(outDir);
        assertEquals(2, files.length);

        String filename1 = files[0].getPath().getName();
        assertEquals(String.format("Filename differ %s", filename1), "4.95", filename1);
        String filename2 = files[1].getPath().getName();
        assertEquals(String.format("Filename differ %s", filename2), "4.71", filename2);

        Path filePath = new Path(outDir + "/" + filename1);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)));

        // verify results sorted
        Integer previousRating = Integer.MAX_VALUE;
        while (reader.readLine() != null){
            String line = reader.readLine();
            String[] lineElements = line.split("\t");

            assertEquals(String.format("There are %d in a line. But 2 expected", lineElements.length),
                    2, lineElements.length);

            Integer currentRating = Integer.parseInt(lineElements[0]);
            assertTrue("Rating is not sorted", currentRating <= previousRating);
        }

        // clean up after test case
        fs.delete(inDir, true);
        fs.delete(outDir,true);
    }


    private void writeHDFSContent(FileSystem fs, Path dir, String fileName, List<String> content) throws IOException {
        Path newFilePath = new Path(dir, fileName);
        FSDataOutputStream out = fs.create(newFilePath);
        for (String line : content){
            out.writeBytes(line);
        }
        out.close();
    }

}
