package table;

import java.util.ArrayList;
import java.util.List;

public class Record {

    List<String> columnValues = new ArrayList<>();

    @Override
    public String toString() {
        return "Record{" +
                "columnValues=" + columnValues +
                '}';
    }
}
