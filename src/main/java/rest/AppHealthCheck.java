package rest;


import com.codahale.metrics.health.HealthCheck;

public class AppHealthCheck extends HealthCheck


{

    ServerIntegrityCheck integrityCheck = new ServerIntegrityCheck();

    protected com.codahale.metrics.health.HealthCheck.Result check() throws Exception
    {

        if (integrityCheck.integrityCheck())
         return com.codahale.metrics.health.HealthCheck.Result.healthy();
        else
            return Result.unhealthy(integrityCheck.errorMessage());
    }

}