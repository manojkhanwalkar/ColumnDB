package query;

public class MetaRequest {

    MetaRequestType type ;

    TableMetaData metaData;

    public MetaRequestType getType() {
        return type;
    }

    public void setType(MetaRequestType type) {
        this.type = type;
    }

    public TableMetaData getMetaData() {
        return metaData;
    }

    public void setMetaData(TableMetaData metaData) {
        this.metaData = metaData;
    }
}
