package SupnuevoPrice;

import weka.clusterers.EM;
import weka.clusterers.SimpleKMeans;
import weka.core.DistanceFunction;
import weka.core.Instances;
import weka.core.ManhattanDistance;
import weka.core.converters.ArffLoader;

import java.io.File;
import java.sql.*;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class DataSuggestMT {

    static void Suggest() throws InterruptedException {
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(8);

        //double CPI = 1.02;

        /*try {
            DataUtils client = new DataUtils();
            Connection conn = client.openDb();
            Statement stms = conn.createStatement();
            String sqls = "truncate table supnuevo_common_commodity_suggestprice";
            stms.execute(sqls);
            stms.close();
        } catch (Exception e) {
            e.printStackTrace();
        }*/


        DistanceFunction disFun = new ManhattanDistance();
        String path = DataUtils.arffCodigoPath;
        File[] files = new File(path).listFiles();
        CountDownLatch cdl = new CountDownLatch(files.length);
        System.out.print("[");
        for (File fl : files) {
            fixedThreadPool.execute(() -> {
                try {
                    System.out.print(">");
                    String codigo = fl.getName().split("\\.")[0];
                    DataUtils client = new DataUtils();
                    int commodityId = 0, suggestLevel = 0;
                    double suggestPrice = 0;
                    String descripcion = null;
                    Connection connsup = client.opensupDb();
                    Statement stms = connsup.createStatement();
                    ResultSet res = stms.executeQuery(String.format("SELECT commodityId, suggestPrice, suggestLevel, nombre FROM supnuevo_common_commodity where Codigo = '%s'", codigo));
                    while (res.next()) {
                        commodityId = res.getInt(1);
                        suggestPrice = res.getDouble(2);
                        suggestLevel = res.getInt(3);
                        descripcion = res.getString(4);
                        if (descripcion != null) descripcion = descripcion.replace("\"", "'").replace("\\", "");
                    }
                    stms.close();
                    connsup.close();
                    ArffLoader loader = new ArffLoader();
                    loader.setFile(fl);
                    Instances ins = loader.getDataSet();
                    EM em = new EM();
                    em.buildClusterer(ins);
                    double[][][] attribute = em.getClusterModelsNumericAtts();
                    double[] prior = em.getClusterPriors();
                    StringBuilder GMMprice = new StringBuilder();
                    StringBuilder GMMpercent = new StringBuilder();
                    int emCluster = em.numberOfClusters();
                    double GMMsuggest = 0, EMmaxPrice = 0, EMmaxPrior = 0, EMmaxPriorPrice = 0;
                    for (int i = 0; i < emCluster; i++) {
                        GMMprice.append(attribute[i][0][0]);
                        GMMprice.append(",");
                        GMMpercent.append(prior[i]);
                        GMMpercent.append(",");
                        if (attribute[i][0][0] > EMmaxPrice) EMmaxPrice = attribute[i][0][0];
                        if (prior[i] > EMmaxPrior) EMmaxPrior = prior[i];
                        if (prior[i] >= 0.75) GMMsuggest = attribute[i][0][0];
                        if (prior[i] >= 0.4) {
                            if (attribute[i][0][0] > EMmaxPriorPrice) EMmaxPriorPrice = attribute[i][0][0];
                        }
                    }
                    if (GMMsuggest == 0) GMMsuggest = EMmaxPriorPrice;
                    if (EMmaxPrior < 0.4) {
                        GMMsuggest = EMmaxPrice;
                    }


                    SimpleKMeans KM = new SimpleKMeans();
                    KM.setDistanceFunction(disFun);
                    if (emCluster == 1) {
                        emCluster = 2;
                    }
                    KM.setNumClusters(emCluster);
                    KM.buildClusterer(ins);
                    double[] centroids = KM.getClusterCentroids().attributeToDoubleArray(0);
                    int[] data = KM.getClusterSizes();
                    double[] percent = new double[KM.getNumClusters()];
                    double sumData = Arrays.stream(data).sum();
                    StringBuilder KmeansPrice = new StringBuilder();
                    StringBuilder KmeansPercent = new StringBuilder();
                    double KmeansSuggest = 0, KmeansMaxPrice = 0, KmeansMaxPrior = 0;
                    for (int i = 0; i < KM.getNumClusters(); i++) {
                        percent[i] = data[i] / sumData;
                        KmeansPrice.append(centroids[i]);
                        KmeansPrice.append(",");
                        KmeansPercent.append(percent[i]);
                        KmeansPercent.append(",");
                        if (centroids[i] > KmeansMaxPrice) KmeansMaxPrice = centroids[i];
                        if (percent[i] > KmeansMaxPrior) KmeansMaxPrior = percent[i];
                        if (percent[i] >= 0.8) KmeansSuggest = centroids[i];
                    }
                    if (KmeansMaxPrior < 0.8) {
                        KmeansSuggest = KmeansMaxPrice;
                    }
                    double suggestPrice1 = 0;
                    double price = (GMMsuggest + KmeansSuggest) / 2;
                    DecimalFormat df = new DecimalFormat("#.0");
                    double newSuggestPrice = Double.parseDouble(df.format(price));

                    GMMsuggest = Double.parseDouble(df.format(GMMsuggest));
                    KmeansSuggest = Double.parseDouble(df.format(KmeansSuggest));
                    if (Math.abs(GMMsuggest - KmeansSuggest) / Math.min(GMMsuggest, KmeansSuggest) < 0.2 && sumData > 40) {
                        suggestPrice1 = newSuggestPrice;
                    }
                    if (suggestLevel == 3 && suggestPrice1 != 0) {
                        suggestPrice = suggestPrice1;
                    }
                    double suggestPriceABS = 0;
                    if (suggestPrice != 0 && suggestPrice1 != 0) {
                        suggestPriceABS = Double.parseDouble(df.format(Math.abs(suggestPrice - suggestPrice1)));
                    }
                    Connection conn = client.openDb();
                    Statement stm = conn.createStatement();
                    String sql = String.format("insert into supnuevo_common_commodity_suggestprice ( commodityId, Codigo, descripcion, suggestPrice, suggestLevel, newSuggestPrice, GMMsuggest, " +
                                    "KmeansSuggest, GMMprice, GMMpercent, KmeansPrice, KmeansPercent, priceCount, suggestPrice1 ) values ( %d, '%s', \"%s\", %s, %d, %s, %s, %s, '%s', '%s', '%s', '%s', %s, %s) "
                            , commodityId, codigo, descripcion, suggestPrice, suggestLevel, suggestPriceABS, GMMsuggest, KmeansSuggest, GMMprice.toString(), GMMpercent.toString()
                            , KmeansPrice.toString(), KmeansPercent.toString(), sumData, suggestPrice1);
                    stm.execute(sql);
                    stm.close();
                    conn.close();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    cdl.countDown();
                }
            });
        }
        cdl.await();
        fixedThreadPool.shutdown();
        if(fixedThreadPool.awaitTermination(60, TimeUnit.MINUTES)) {
            System.out.println("mission success!");
        } else {
            System.out.println("some mission not completion in a hour!");
        }
        System.out.print("]");
    }

}
