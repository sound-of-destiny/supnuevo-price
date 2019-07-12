package SupnuevoPrice;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

class DataExcute {

    static void syncDataBase() {
        DataUtils.Backupdbfromsql();
        DataUtils.Restoredbtosql();
    }

    static void deleteTimes(int startDate, int endDate) {
        String path = DataUtils.dataPath;
        for (; endDate > startDate; endDate--) {
            String datetime = endDate + "";
            File file = new File(path + "/" + datetime);
            File[] merList = file.listFiles();
            if (merList != null) {
                for (File mer : merList) {
                    File[] timesList = mer.listFiles();
                    if (timesList != null) {
                        int last = timesList.length;
                        for (File times : timesList) {
                            int name = Integer.parseInt(times.getName().split("\\.")[0]);
                            if (name < last) times.delete();
                        }
                    }
                }
            }
        }
    }

    static void deleteMer(int startDate, int endDate) {
        ArrayList<Integer> merchantList = new ArrayList<>();
        try {
            DataUtils client = new DataUtils();
            Connection conn = client.openDb();
            Statement stm = conn.createStatement();
            String sql1 = "SELECT distinct merchantId FROM supnuevo_merchant_info a join supnuevo_province_city b " +
                    "where b.cityId=a.cityId and (b.provinceId=2 or b.provinceId=3);";
            ResultSet merchantIdRes = stm.executeQuery(sql1);
            while (merchantIdRes.next()) {
                merchantList.add(merchantIdRes.getInt(1));
            }
            stm.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        String path = DataUtils.dataPath;
        int merchantLen = merchantList.size();

        for (; endDate > startDate; endDate--) {
            String datetime = endDate + "";
            File file = new File(path + "/" + datetime);
            File[] merList = file.listFiles();
            if (merList != null) {
                for (File mer : merList) {
                    int merId = Integer.parseInt(mer.getName());
                    int j = 0;
                    for (int m : merchantList) {
                        j = j + 1;
                        if (merId == m) break;
                        if (j == merchantLen) {
                            File[] timesList = mer.listFiles();
                            if (timesList != null) {
                                for (File times : timesList) times.delete();
                            }
                            mer.delete();
                        }
                    }
                }
            }
        }
    }



    static void toARRF() throws Exception {
        String dataPath = DataUtils.dataPath;
        String newDataPath = DataUtils.newDataPath;
        String outPathARFF = DataUtils.arffCodigoPath;

        Path ol = Path.of(outPathARFF);
        Files.list(ol).map(Path::toFile).forEach(File::delete);
        if (!Files.exists(ol)) {
            Files.createDirectory(ol);
        }

        final FileChannel channel = new FileInputStream(Path.of(newDataPath).toFile()).getChannel();
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        List<Codigo> list = new ArrayList<>();
        while(buffer.hasRemaining()) {
            int len = buffer.getInt();
            byte[] c = new byte[len];
            buffer.get(c);
            Codigo codigo = new Codigo();
            codigo.setCodigoId(new String(c));
            codigo.setMerchant(buffer.getInt());
            codigo.setPrice(buffer.getDouble());
            codigo.setTime(0);
            list.add(codigo);
        }
        channel.close();

        HashMap<String, Integer> codigoMap = new HashMap<>();
        DataUtils client = new DataUtils();
        Connection conn = client.openDb();
        Statement stm = conn.createStatement();
        String sql = "SELECT codigo FROM supnuevo_common_commodity where modifyTime > 20180101";
        ResultSet codigoRes = stm.executeQuery(sql);
        while (codigoRes.next()) {
            codigoMap.put(codigoRes.getString(1), 0);
        }
        stm.close();
        conn.close();

        Path dl = Path.of(dataPath);
        Files.walk(dl).map(Path::toFile).forEach(file -> {
            if (!file.isDirectory()) {
                String[] codigos;
                int[] cods;
                double[] prics;
                try (FileInputStream in = new FileInputStream(file);
                     ObjectInputStream oin = new ObjectInputStream(in)) {
                    codigos = (String[]) oin.readObject();
                    cods = (int[]) oin.readObject();
                    prics = (double[]) oin.readObject();
                    int len = oin.readInt();
                    for (int i = 0; i < len; i++) {
                        if (codigoMap.get(codigos[i]) != null) {
                            if (prics[i] >= 5) {
                                Codigo codigo = new Codigo();
                                codigo.setCodigoId(codigos[i]);
                                codigo.setPrice(prics[i]);
                                String[] p = file.getParent().split("/");
                                codigo.setMerchant(Integer.parseInt(p[p.length - 1]));
                                codigo.setTime(Integer.parseInt(p[p.length - 2]));
                                list.add(codigo);
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });
        System.out.println(LocalDateTime.now());
        Map<String, Map<Integer, Double>> result = new HashMap<>();
        Map<String, List<Codigo>> map = list.parallelStream().collect(Collectors.groupingBy(Codigo::getCodigoId));
        map.forEach((codigo, codigos) -> {
            Map<Integer, Double> innermap = new HashMap<>();
            Map<Integer, List<Codigo>> map2 = codigos.parallelStream().collect(Collectors.groupingBy(Codigo::getMerchant));
            map2.forEach((mer, mers) -> {
                Optional<Codigo> c = mers.parallelStream().max(Comparator.comparingInt(Codigo::getTime));
                c.ifPresent(c1 -> innermap.put(mer, c1.getPrice()));
            });
            result.put(codigo, innermap);
        });

        DecimalFormat df = new DecimalFormat("#.0");
        for (String codigo : result.keySet()) {
            File arff = new File(outPathARFF + codigo + ".arff");
            BufferedWriter bw = new BufferedWriter(new FileWriter(arff, true));
            bw.write("@RELATION " + codigo);
            bw.newLine();
            bw.newLine();
            bw.write("@ATTRIBUTE price REAL");
            bw.newLine();
            bw.newLine();
            bw.write("@DATA");
            bw.newLine();

            for(Integer merchant : result.get(codigo).keySet()) {
                double price = result.get(codigo).get(merchant);
                double nowPrice = Double.parseDouble(df.format(price));
                bw.write(nowPrice + "");
                bw.newLine();
            }
            bw.close();
        }

        File arff = new File(newDataPath);
        try (FileOutputStream fos = new FileOutputStream(arff);
             DataOutputStream baos = new DataOutputStream(fos)) {
            for (String codigo : result.keySet()) {
                for(Integer merchant : result.get(codigo).keySet()) {
                    double price = result.get(codigo).get(merchant);
                    baos.writeInt(codigo.length());
                    baos.writeBytes(codigo);
                    baos.writeInt(merchant);
                    baos.writeDouble(price);
                }

            }
        }
    }

}
