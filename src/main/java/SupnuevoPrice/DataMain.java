package SupnuevoPrice;

public class DataMain {

    public static void main(String[] args) throws Exception {
        System.out.println("Program Start, Please Wait...");
        //int yesterday = Integer.parseInt(args[0]);
        //int lastSunday = Integer.parseInt(args[1]);
        //DataMain.pre(lastSunday, yesterday);
        System.out.println("Start Data Execution");
        //DataMain.excute();
        System.out.println("Start Price Suggested");
        DataMain.suggest();
        System.out.println("Mission Complete!");
    }

    private static void pre(int startDate, int endDate) {
        DataExcute.deleteTimes(startDate, endDate);
        DataExcute.deleteMer(startDate, endDate);
        DataExcute.syncDataBase();
    }

    private static void excute() throws Exception {
        DataExcute.toARRF();
    }

    private static void suggest() throws Exception {
        DataSuggestMT.Suggest();
    }
}
