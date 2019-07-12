package SupnuevoPrice;

public class ProgressBar {

    private double finishPoint;
    private double barLength;

    public ProgressBar(){
        this.finishPoint = 100;
        this.barLength = 20;
    }

    public ProgressBar(double finishPoint, int barLength){
        this.finishPoint = finishPoint;
        this.barLength = barLength;
    }

    public void showBarByPoint(double currentPoint) {
        double rate = currentPoint / this.finishPoint;
        int barSign = (int) (rate * this.barLength);
        System.out.print("\r");
        System.out.print(makeBarBySignAndLength(barSign) + String.format(" %.2f%%", rate * 100));
    }

    private String makeBarBySignAndLength(int barSign) {
        StringBuilder bar = new StringBuilder();
        bar.append("[");
        for (int i=1; i<=this.barLength; i++) {
            if (i < barSign) {
                bar.append("-");
            } else if (i == barSign) {
                bar.append(">");
            } else {
                bar.append(" ");
            }
        }
        bar.append("]");
        return bar.toString();
    }
}

