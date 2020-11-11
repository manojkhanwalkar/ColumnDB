package query;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataContainer {

    Map<String,List<String>> values = new HashMap<>();

    public Map<String, List<String>> getValues() {
        return values;
    }

    public void setValues(Map<String, List<String>> values) {
        this.values = values;
    }

    public void addValues(String columnName , List<String> value)
    {
        values.put(columnName,value);
    }


    @Override
    public String toString() {
        return "DataContainer{" +
                "values=" + values +
                '}';
    }
}
