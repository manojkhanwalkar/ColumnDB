package rest;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DBLocks {

    public static DBLocks getInstance()
    {
        return Holder.INSTANCE;
    }





    private DBLocks()
    {
        locks = new ConcurrentHashMap<>();
    }

    ConcurrentMap<String,ConcurrentMap<String, ReadWriteLock> >locks ;

    public void createLocks(String dir) {
        // get all databases and for each database get all tables .
        File rootFile = new File(dir);

        Arrays.stream(rootFile.listFiles()).forEach(database->{
            locks.put(database.getName(),new ConcurrentHashMap<>());
            Arrays.stream(database.listFiles()).forEach(file->{
                locks.get(database.getName()).put(file.getName(),new ReentrantReadWriteLock());
            });
        });
    }


    public ReadWriteLock get(String databaseName , String tableName)
    {
        return locks.get(databaseName).get(tableName);
    }

    public void createLock(String databaseName) {

        locks.put(databaseName,new ConcurrentHashMap<>());
    }


    public void createLock(String databaseName, String tableName) {
        locks.get(databaseName).put(tableName,new ReentrantReadWriteLock());


    }

    public void deleteLock(String databaseName) {

        locks.remove(databaseName);
    }

    public void deleteLock(String databaseName,String tableName) {

        locks.get(databaseName).remove(tableName);
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
