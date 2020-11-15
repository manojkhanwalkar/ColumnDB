package marketing;

import client.ColumnDBClient;
import person.BatchLoader;
import query.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ActivityDataLoader {



    public static void main(String[] args) {

        BatchLoader loader = new BatchLoader("/home/manoj/data/ColumnDB/activity.csv", "marketing", "activity");


        loader.loadMetaData();

        DataContainer datacontainer = loader.loadData();

        loader.load(datacontainer);

    }


}
