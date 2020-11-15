package rest.pool;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;

public class ColumnReader {

    String name;

    BufferedReader bufferedReader ;
    FileInputStream fis=null;
    public ColumnReader(String name)
    {
        this.name = name;


        try {
            fis = new FileInputStream(name);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        bufferedReader = new BufferedReader(new InputStreamReader(fis));

    }

    public BufferedReader getBufferedReader() {
        return bufferedReader;
    }

    public void setBufferedReader(BufferedReader bufferedReader) {
        this.bufferedReader = bufferedReader;
    }
}
