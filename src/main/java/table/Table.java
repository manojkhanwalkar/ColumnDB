package table;

import query.DataContainer;

import java.util.ArrayList;
import java.util.List;

public class Table {

    List<String> columnNames = new ArrayList<>();

    List<Record> records = new ArrayList<>(); ;


    public void process(List<DataContainer> dataContainers)
    {
        columnNames.addAll(dataContainers.get(0).getValues().keySet());

        int size = dataContainers.get(0).getValues().values().stream().findFirst().get().size(); // size of the rows
        int cols = columnNames.size();


        dataContainers.stream().forEach(container->{

            List<List<String>> x = new ArrayList<>();

            x.addAll(container.getValues().values());


            for (int i=0;i<size;i++)
            {
                Record record = new Record();
                for (int j=0;j<cols;j++)
                {
                    record.columnValues.add(x.get(j).get(i));
                }

                records.add(record);
            }


        });

    }


    @Override
    public String toString() {
        return "Table{" +
                "columnNames=" + columnNames +
                ", records=" + records +
                '}';
    }
}
