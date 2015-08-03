import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.Random;

/**
 * Created by mkhanwalkar on 8/2/15.
 */
public class PersonDataCreator {

    public static void main(String[] args) {
        PersonDataCreator dataCreator = new PersonDataCreator();

        dataCreator.create();
    }

    Random random = new Random();

    private int getNextInt(int start , int end)
    {
        return start + random.nextInt(end);
    }


    private void create() {

        try {

            PrintWriter writer = new PrintWriter("/tmp/person.csv");
            writer.write("age,gender,zip,income,state\n");

            for (int i=0;i<100;i++) {
                writer.write(getAge() + ",");
                writer.write(getGender() + ",");
                writer.write(getZip() + ",");
                writer.write(getIncome() + ",");
                writer.write(getState() + "\n");

            }

            writer.flush();
            writer.close();


        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    private int getAge()
    {
        return getNextInt(1,99);
    }

    private int getIncome()
    {
        return getNextInt(10000,1000000);
    }

    String[] states = {"CA","PA","NY","NJ", "MD", "DA","AL","HA","MD"};

    private String getState() {
        return states[getNextInt(0, states.length - 1)];
    }

    private String getZip() {

        int zip = getNextInt(10000,99999);
        return String.valueOf(zip);
    }

    String[] genders = {"M", "F"};

    private String getGender() {
            return genders[getNextInt(0,100000)%2];
    }


}
