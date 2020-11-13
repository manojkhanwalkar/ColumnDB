package rest;


import com.codahale.metrics.health.HealthCheck;

import static rest.ColumnResource.seperator;

public class AppHealthCheck extends HealthCheck


{

    ServerIntegrityCheck integrityCheck = new ServerIntegrityCheck(ColumnResource.rootDirName+seperator+ColumnResource.clusterName);

    protected com.codahale.metrics.health.HealthCheck.Result check() throws Exception
    {

        if (integrityCheck.integrityCheck())
         return com.codahale.metrics.health.HealthCheck.Result.healthy();
        else
            return Result.unhealthy(integrityCheck.errorMessage());
    }

}