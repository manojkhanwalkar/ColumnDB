package rest;

import query.Request;
import query.Response;
import server.Server;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

@Path("/columndb")
@Produces(MediaType.APPLICATION_JSON)
public class ColumnResource {

    public ColumnResource() {
    }


 /*

    @Path(value = "/matchdevice")
    @Produces(MediaType.APPLICATION_JSON)
    public Response matchDevice(@Context HttpServletRequest hsReq, @Valid final MatchDeviceRequest request) throws Exception {
        final Boolean enableRestEndPoint = (Boolean) ApiConfig.INSTANCE.getValue(ApiConfig.ENABLE_MATCH_DEVICE);
  */

    @POST
    public Response createTable(@Context HttpServletRequest hsReq, @Valid Request request) {


       // GraphDB db = ((DBService) Server.getService("DBService")).getDatabase(request.getDbName());
        Response response=null;

    /*    switch (request.getOperation())
        {
            case Query:
              response   = db.query(request);
              break ;
            case AddNode:
                Node n = db.createOrGetNode(request.getId());
                response = new Response();
                response.setNode(n);
                break;
            case AddRelation: {
                Node n1 = db.createOrGetNode(request.getId());
                Node n2 = db.createOrGetNode(request.getTgtId());
                db.addRelationship(n1, n2);
                break;
            }
            case DeleteRelation: {
                db.deleteRelationship(request.getId(), request.getTgtId());
                break;
            }
            case DeleteNode:
                db.deleteNode(request.getId());
            default :
                response = null;

        }*/


        return response;
    }
}