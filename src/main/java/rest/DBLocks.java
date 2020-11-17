package rest;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DBLocks {

    public static DBLocks getInstance()
    {
        return Holder.INSTANCE;
    }


   public enum Type { Read , Write} ;


    private DBLocks()
    {
        tableLocks = new ConcurrentHashMap<>();
    }

    ConcurrentMap<String,ConcurrentMap<String, ReadWriteLock>> tableLocks;



    public void createLocks(String dir) {
        // get all databases and for each database get all tables .
        File rootFile = new File(dir);

        Arrays.stream(rootFile.listFiles()).forEach(database->{
            tableLocks.put(database.getName(),new ConcurrentHashMap<>());
            Arrays.stream(database.listFiles()).forEach(file->{
                tableLocks.get(database.getName()).put(file.getName(),new ReentrantReadWriteLock());
            });
        });
    }


/*    public ReadWriteLock get(String databaseName , String tableName)
    {
        return tableLocks.get(databaseName).get(tableName);
    }*/



    public void lock(String databaseName, String tableName , Type type)
    {
        var lock = tableLocks.get(databaseName).get(tableName);

        ((type==Type.Write)?lock.writeLock():lock.readLock()).lock();

    }

    public void unlock(String databaseName, String tableName , Type type)
    {
        var lock = tableLocks.get(databaseName).get(tableName);

        ((type==Type.Write)?lock.writeLock():lock.readLock()).unlock();

    }



    public void createLock(String databaseName) {

        tableLocks.computeIfAbsent(databaseName,d->new ConcurrentHashMap<>());
    }


    public void createLock(String databaseName, String tableName) {
        tableLocks.get(databaseName).computeIfAbsent(tableName,t->new ReentrantReadWriteLock());


    }

    public void deleteLock(String databaseName) {

        tableLocks.remove(databaseName);
    }

    public void deleteLock(String databaseName,String tableName) {

        tableLocks.get(databaseName).remove(tableName);
    }



    static class Holder
    {
        static DBLocks INSTANCE = new DBLocks();

    }

    @Override
    public String toString() {
        return "DBLocks{" +
                "tableLocks=" + tableLocks +
                '}';
    }
}
