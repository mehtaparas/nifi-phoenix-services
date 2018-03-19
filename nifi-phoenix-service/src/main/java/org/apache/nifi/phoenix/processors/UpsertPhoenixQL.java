package org.apache.nifi.phoenix.processors;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.TextNode;
import org.apache.nifi.phoenix.service.PhoenixDBCPService;

public class UpsertPhoenixQL extends AbstractProcessor{

    //Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from SQL query result set.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("SQL query execution failed. Incoming FlowFile will be penalized and routed to this relationship")
            .build();

    public static final PropertyDescriptor PHOENIX_DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Phoenix Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to Phoenix")
            .required(true)
            .identifiesControllerService(PhoenixDBCPService.class).build();


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException{

        //Verify the Flow file
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final PhoenixDBCPService dbcpService = context.getProperty(PHOENIX_DBCP_SERVICE).asControllerService(PhoenixDBCPService.class);

        //parse the JSON content
        final ObjectMapper mapper = new ObjectMapper();
        final AtomicReference<JsonNode> rootNodeRef = new AtomicReference<>(null);
        final ArrayList<String> upsert_queries = new ArrayList<String>();
        try {
            session.read(flowFile, new InputStreamCallback() {

                @Override
                public void process(final InputStream in) throws IOException {
                    try (final InputStream bufferedIn = new BufferedInputStream(in)) {
                        rootNodeRef.set(mapper.readTree(bufferedIn));
                    }
                }
            });
        } catch (final ProcessException pe) {
            getLogger().error("Failed to parse {} as JSON due to {}; routing to failure", new Object[] {flowFile, pe.toString()}, pe);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
        final JsonNode rootNode = rootNodeRef.get();

        try{
            // The node may or may not be a Json Array. If it isn't, we will create an
            // ArrayNode and add just the root node to it. We do this so that we can easily iterate
            // over the array node, rather than duplicating the logic or creating another function that takes many variables
            // in order to implement the logic.
            final ArrayNode arrayNode;
            if (rootNode.isArray()) {
                arrayNode = (ArrayNode) rootNode;
            } else {
                final JsonNodeFactory nodeFactory = JsonNodeFactory.instance;
                arrayNode = new ArrayNode(nodeFactory);
                arrayNode.add(rootNode);
            }

            //iterate through the JSON Array objects (for each record)
            for (int i=0; i < arrayNode.size(); i++) {
                //generate upsert statements
                final StringBuilder sqlBuilder = new StringBuilder();
                sqlBuilder.append("UPSERT INTO ");
                String tableName = "TEST_PHEONIX"; //comes as Nifi Attribute
                sqlBuilder.append(tableName); //comes from Nifi Attribute

                final JsonNode jsonNode = arrayNode.get(i);
                final StringBuilder columnBuilder = new StringBuilder();
                final StringBuilder valueBuilder = new StringBuilder();
                columnBuilder.append(" (");
                valueBuilder.append(" (");
                int fieldCount=0;
                //Fetch the field names and respective values
                final Set<String> normalizedFieldNames = getNormalizedColumnNames(jsonNode);
                Iterator<String> itr = normalizedFieldNames.iterator();
                while(itr.hasNext()){
                    String fieldname=itr.next();
                    if (fieldCount++ > 0) {
                        columnBuilder.append(", ");
                        valueBuilder.append(",");
                    }
                    columnBuilder.append(fieldname);

                    if(jsonNode.get(fieldname) instanceof TextNode){
                        String temp="'"+jsonNode.get(fieldname).asText()+"'";
                        valueBuilder.append(temp);
                    }
                    else {
                        valueBuilder.append(jsonNode.get(fieldname));
                    }
                }
                columnBuilder.append(")");
                valueBuilder.append(")");
                sqlBuilder.append(columnBuilder).append(" VALUES").append(valueBuilder);
                upsert_queries.add(sqlBuilder.toString());
                //System.out.println("sqlBuilder: " +sqlBuilder);
            }

        }catch(Exception e){
            System.out.println("Exception: "+e);

        }

        //Executing queries with Batch job
        try (final Connection con = dbcpService.getConnection();
             final Statement st = con.createStatement()) {
            for(String sql:upsert_queries){
                System.out.println("sqlBuilder: " +sql);
                st.addBatch(sql);

            }
            //Execute the batch processing
            st.executeBatch();
            con.commit();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    //Method to fetch the column names for the given flowfile JSON data
    private Set<String> getNormalizedColumnNames(final JsonNode node) {
        final Set<String> normalizedFieldNames = new HashSet<String>();
        final Iterator<String> fieldNameItr = node.getFieldNames();
        while (fieldNameItr.hasNext()) {
            normalizedFieldNames.add(fieldNameItr.next());
        }

        return normalizedFieldNames;
    }

}
