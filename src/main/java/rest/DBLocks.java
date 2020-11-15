package rest;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;

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
    }


    static class Holder
    {
        static DBLocks INSTANCE = new DBLocks();

    }
}
