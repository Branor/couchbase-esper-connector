package example;

/**
 * Created by David on 03/02/2015.
 */
public class Data {
    private String text;
    private double number;

    public Data() {

    }

    public Data(String text, double number) {
        this.setText(text);
        this.setNumber(number);
    }

    public String getText() {
        return text;
    }

    public double getNumber() {
        return number;
    }

    public void setText(String text) {
        this.text = text;
    }

    public void setNumber(double number) {
        this.number = number;
    }

    @Override
    public String toString() {
        return "text: " + text + ", number: " + number;
    }
}
