package test;

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

public class Test {

    /*void toNARFF() throws Exception {
        String path = DataUtils.dataPath;
        String outPathARFF = DataUtils.arffCodigoPath;

        *//*File ol = new File(outPathARFF);
        if (!ol.exists()) {
            ol.mkdirs();
        }
        Optional<File[]> files = Optional.ofNullable(ol.listFiles());
        files.ifPresent((foles) -> Arrays.stream(foles).forEach(File::delete));*//*

        Path ol = Path.of(outPathARFF);
        Files.list(ol).map(Path::toFile).forEach(File::delete);
        if (!Files.exists(ol)) {
            Files.createDirectory(ol);
        }



        *//*final FileChannel channel = new FileInputStream(Path.of(path).toFile()).getChannel();
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        buffer.
        channel.close();*//*

        System.out.println(LocalDateTime.now());

        List<Codigo> list = new ArrayList<>();
        *//*Path dl = Path.of(path);
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
                                //codigo.setCodigoId(codigos[i]);
                                codigo.setPrice(prics[i]);
                                String[] p = file.getParent().split("/");
                                codigo.setMerchant(Integer.parseInt(p[p.length - 1]));
                                codigo.setTime(Integer.parseInt(p[p.length - 2]));
                                list.add(codigo);
                                p = null;
                            }
                        }
                    }
                    codigos = null;
                    cods = null;
                    prics = null;
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });*//*

    }*/

    /*void toARRF() throws Exception {
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
            codigo.setTime(20190113);
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
    }*/

    /*void toCSV(int startDate, int endDate) {
        String path = DataUtils.dataPath;
        String outPathARFF = DataUtils.arffCodigoPath;

        File ol = new File(outPathARFF);
        if (!ol.exists()) {
            ol.mkdirs();
        }
        File[] olist = ol.listFiles();
        if (olist != null) {
            for(File of : olist){
                of.delete();
            }
        }

        File d = new File(path);
        File[] fl = d.listFiles();
        int index = 0;
        ProgressBar pb = new ProgressBar(fl.length, 60);
        pb.showBarByPoint(index);
        try {
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

            HashMap<String, HashMap> codigoTable = new HashMap<>();
            for (; endDate > startDate; endDate--) {
                File[] merList = new File(path + "/" + endDate).listFiles();
                if(merList==null)continue;
                pb.showBarByPoint(index++);
                for (File mer : merList) {
                    String[] codigos = {};
                    int[] cods = {};
                    double[] prics = {};
                    int len = 0;
                    int merchant = Integer.parseInt(mer.getName());
                    File[] timesList = mer.listFiles();
                    if(timesList == null)continue;
                    for (File times : timesList) {
                        FileInputStream in = new FileInputStream(times);
                        ObjectInputStream oin = new ObjectInputStream(in);
                        try{
                            codigos = (String[]) oin.readObject();
                            cods = (int[]) oin.readObject();
                            prics = (double[]) oin.readObject();
                            len = oin.readInt();
                        } catch (EOFException e){
                            continue;
                        }
                        oin.close();
                        in.close();
                        for (int j = 0; j < len; j++) {
                            if (codigoMap.get(codigos[j]) != null) {
                                double price = prics[j];
                                if (price >= 5) {
                                    HashMap map = codigoTable.get(codigos[j]);
                                    if (map == null) {
                                        HashMap<Integer, Double> priceTable = new HashMap<>();
                                        DecimalFormat df = new DecimalFormat("#.0");
                                        double nowPrice = Double.parseDouble(df.format(price));
                                        priceTable.put(merchant, nowPrice);
                                        codigoTable.put(codigos[j], priceTable);
                                    } else if (map.get(merchant) == null){
                                        DecimalFormat df = new DecimalFormat("#.0");
                                        double nowPrice = Double.parseDouble(df.format(price));
                                        map.put(merchant, nowPrice);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            for (String codigo : codigoTable.keySet()) {
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

                for(Object merchant : codigoTable.get(codigo).keySet()) {
                    double price = (double)codigoTable.get(codigo).get(merchant);
                    bw.write(price + "");
                    bw.newLine();
                }

                bw.close();
            }

            File arff = new File("/home/schong/Documents/out.data");
            try (FileOutputStream fos = new FileOutputStream(arff);
                 DataOutputStream baos = new DataOutputStream(fos)) {
                for (String codigo : codigoTable.keySet()) {

                    for(Object merchant : codigoTable.get(codigo).keySet()) {
                        double price = (double)codigoTable.get(codigo).get(merchant);
                        baos.writeInt(codigo.length());
                        baos.writeBytes(codigo);
                        baos.writeInt((int)merchant);
                        baos.writeDouble(price);
                    }

                }
            }
            File arff2 = new File("/home/schong/Documents/out2.data");
            try (FileOutputStream fos = new FileOutputStream(arff2);
                 DataOutputStream baos = new DataOutputStream(fos)) {
                for (String codigo : codigoTable.keySet()) {

                    for(Object merchant : codigoTable.get(codigo).keySet()) {
                        double price = (double)codigoTable.get(codigo).get(merchant);
                        baos.writeLong(Long.parseLong(codigo));
                        baos.writeInt((int)merchant);
                        baos.writeDouble(price);
                    }

                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }*/



}
