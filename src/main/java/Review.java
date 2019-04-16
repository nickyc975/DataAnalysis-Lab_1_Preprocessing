import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;

public class Review implements Writable, Cloneable {
    private static final Map<String, String> MONTHS;
    private static final String TEMPLATE = "%s|%f|%f|%f|%s|%s|%f|%s|%s|%s|%s|%f";

    static {
        MONTHS = new HashMap<>();
        MONTHS.put("january", "1");
        MONTHS.put("february", "2");
        MONTHS.put("march", "3");
        MONTHS.put("april", "4");
        MONTHS.put("may", "5");
        MONTHS.put("june", "6");
        MONTHS.put("july", "7");
        MONTHS.put("august", "8");
        MONTHS.put("september", "9");
        MONTHS.put("october", "10");
        MONTHS.put("november", "11");
        MONTHS.put("december", "12");
    }

    public String review_id;
    public double longitude;
    public double latitude;
    public double altitude;
    public String review_date;
    public String temperature;
    public double rating;
    public String user_id;
    public String user_birthday;
    public String user_nationality;
    public String user_career;
    public double user_income;

    public Review() {
    }

    public void fromString(String str) {
        String[] parts = str.split("\\|");
        this.review_id = parts[0];
        this.longitude = Double.parseDouble(parts[1]);
        this.latitude = Double.parseDouble(parts[2]);
        this.altitude = Double.parseDouble(parts[3]);
        this.review_date = uniformDate(parts[4]);
        this.temperature = uniformTemp(parts[5]);

        if (parts[6].startsWith("?")) {
            this.rating = -1;
        } else {
            this.rating = Double.parseDouble(parts[6]);
        }

        this.user_id = parts[7];
        this.user_birthday = uniformDate(parts[8]);
        this.user_nationality = parts[9];
        this.user_career = parts[10];

        if (parts[11].startsWith("?")) {
            this.user_income = -1;
        } else {
            this.user_income = Double.parseDouble(parts[11]);
        }
    }

    @Override
    public String toString() {
        return String.format(TEMPLATE, 
                this.review_id, this.longitude, this.latitude, this.altitude, this.review_date,
                this.temperature, this.rating, this.user_id, this.user_birthday, this.user_nationality,
                this.user_career, this.user_income
        );
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.review_id = input.readUTF();
        this.longitude = input.readDouble();
        this.latitude = input.readDouble();
        this.altitude = input.readDouble();
        this.review_date = input.readUTF();
        this.temperature = input.readUTF();
        this.rating = input.readDouble();
        this.user_id = input.readUTF();
        this.user_birthday = input.readUTF();
        this.user_nationality = input.readUTF();
        this.user_career = input.readUTF();
        this.user_income = input.readDouble();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeUTF(this.review_id);
        output.writeDouble(this.longitude);
        output.writeDouble(this.latitude);
        output.writeDouble(this.altitude);
        output.writeUTF(this.review_date);
        output.writeUTF(this.temperature);
        output.writeDouble(this.rating);
        output.writeUTF(this.user_id);
        output.writeUTF(this.user_birthday);
        output.writeUTF(this.user_nationality);
        output.writeUTF(this.user_career);
        output.writeDouble(this.user_income);
    }

    public static Review read(DataInput input) throws IOException {
        Review review = new Review();
        review.readFields(input);
        return review;
    }

    private static String uniformDate(String str) {
        if (str.indexOf('/') >= 0) {
            return String.join("-", str.split("/"));
        } else if (str.indexOf(',') >= 0) {
            String[] tmp = str.split("[, ]");
            return tmp[2] + "-" + MONTHS.get(tmp[0].toLowerCase()) + "-" + tmp[1];
        } else {
            return str;
        }
    }

    private static String uniformTemp(String str) {
        if (str.endsWith("C")) {
            return str;
        }

        String num = str.substring(0, str.length() - 2);
        double celsius = (Double.parseDouble(num) - 32) / 1.8d;
        return String.format("%.1f℃", celsius);
    }
}