package gr.ds.unipi.spatialnodb.messages.common.vre;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.util.Map;

public class VRERecordSegmentPointsReadSupport extends ReadSupport<VRERecord> {

    @Override
    public ReadContext init(InitContext context){

        MessageType schema = MessageTypeParser.parseMessageType( "message VRERecord {\n" +
                "required BINARY objectId;\n" +
                "required INT32 serialNumber;\n" +
                "required BINARY longitude;\n" +
                "required BINARY latitude;\n" +
                "}");

        return new ReadContext(schema/*context.getFileSchema()*/);
    }

    @Override
    public RecordMaterializer<VRERecord> prepareForRead(Configuration configuration, Map<String, String> map, MessageType messageType, ReadContext readContext) {
        return new VRERecordSegmentPointsMaterializer();
    }
}
