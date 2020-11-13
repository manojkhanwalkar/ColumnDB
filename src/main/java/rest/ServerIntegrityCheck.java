package rest;

public class ServerIntegrityCheck {


    boolean check=false;
    public boolean integrityCheck()
    {
        return false;
    }


    String errorMessage = "";
    public String errorMessage() {

        return errorMessage;

    }


    public ServerIntegrityCheck()
    {
        // for every table directory - read the meta file and then check if all columns exist as per the meta file .
        // for each column check if the number of records are the same for all columns in a table.



    }



}
