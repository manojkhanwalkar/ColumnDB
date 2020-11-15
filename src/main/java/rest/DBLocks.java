package rest;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DBLocks {

    public static DBLocks getInstance()
    {
        return Holder.INSTANCE;
    }





    private DBLocks()
    {
        locks = new ConcurrentHashMap<>();
    }

    ConcurrentMap<String,ConcurrentMap<String, Lock>> locks ;

    public void createLocks(String dir) {
        // get all databases and for each database get all tables .
        File rootFile = new File(dir);

        Arrays.stream(rootFile.listFiles()).forEach(database->{
            locks.put(database.getName(),new ConcurrentHashMap<>());
            Arrays.stream(database.listFiles()).forEach(file->{
                locks.get(database.getName()).put(file.getName(),new ReentrantLock());
            });
        });
    }


    static class Holder
    {
        static DBLocks INSTANCE = new DBLocks();

    }

    @Override
    public String toString() {
        return "DBLocks{" +
                "locks=" + locks +
                '}';
    }
}
