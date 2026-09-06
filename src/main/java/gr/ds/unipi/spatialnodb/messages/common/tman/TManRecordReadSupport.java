package gr.ds.unipi.spatialnodb.messages.common.tman;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.util.Map;

public class TManRecordReadSupport extends ReadSupport<TManRecord> {

    @Override
    public ReadContext init(InitContext context){

        MessageType schema = MessageTypeParser.parseMessageType( "message TManRecord {\n" +
                "required BINARY objectId;\n" +
                "required BINARY longitude;\n" +
                "required BINARY latitude;\n" +
                "required INT64 key;\n" +
                "repeated INT32 dpPointIndexes;\n" +
                "required BINARY dpMbrs;\n" +
                "}");

        return new ReadContext(schema);
    }

    @Override
    public RecordMaterializer<TManRecord> prepareForRead(Configuration configuration, Map<String, String> map, MessageType messageType, ReadContext readContext) {
        return new TManRecordMaterializer();
    }
}
