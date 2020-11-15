package marketing;

import java.io.PrintWriter;
import java.util.Random;
import java.util.UUID;


public class SampleDataCreatorForActivity {

    public static void main(String[] args) {
        SampleDataCreatorForActivity dataCreator = new SampleDataCreatorForActivity();

        dataCreator.create();
    }

    Random random = new Random();

    private int getNextInt(int start , int end)
    {
        return start + random.nextInt(end);
    }


    //  String[] columns = { "userid" , "campaignid" , "clickid" , "seen" , "clicked" , "purchased" } ;
    //            int[] sizes = { 10, 20, 36, 1,1,1 } ;

    private void create() {

        try {

            PrintWriter writer = new PrintWriter("/home/manoj/data/ColumnDB/activity.csv");
            writer.write("userid,campaignid,clickid,seen,clicked,purchased\n");

            for (int i=0;i<10000;i++) {
                writer.write(getUserId() + ",");
                writer.write(getCampaignId() + ",");
                writer.write(getClickId() + ",");
                writer.write(getClick() + ",");
                writer.write(getClick() + ",");
                writer.write(getClick() + "\n");

            }

            writer.flush();
            writer.close();


        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    private String getUserId()
    {

        return "User"+getNextInt(1,10000);
    }

    private String getCampaignId()
    {

        return "C"+getNextInt(1,10000);
    }

    private String getClickId()
    {

        return UUID.randomUUID().toString();
    }



    String[] options = {"Y", "N"};

    private String getClick() {
            return options[getNextInt(0,100000)%2];
    }


}
