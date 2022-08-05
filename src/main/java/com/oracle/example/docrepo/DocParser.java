package com.oracle.example.docrepo;

import com.fnproject.fn.api.InputBinding;
import com.oracle.bmc.Region;
import com.oracle.bmc.auth.AuthenticationDetailsProvider;
import com.oracle.bmc.auth.ResourcePrincipalAuthenticationDetailsProvider;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.requests.PutObjectRequest;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;
import com.oracle.bmc.objectstorage.responses.PutObjectResponse;
import com.oracle.bmc.streaming.StreamClient;
import com.oracle.bmc.streaming.model.PutMessagesDetails;
import com.oracle.bmc.streaming.model.PutMessagesDetailsEntry;
import com.oracle.bmc.streaming.model.PutMessagesResultEntry;
import com.oracle.bmc.streaming.model.Stream;
import com.oracle.bmc.streaming.StreamAdminClient;

import com.oracle.bmc.streaming.requests.GetStreamRequest;
import com.oracle.bmc.streaming.requests.ListStreamsRequest;
import com.oracle.bmc.streaming.requests.PutMessagesRequest;
import com.oracle.bmc.streaming.responses.GetStreamResponse;
import com.oracle.bmc.streaming.responses.ListStreamsResponse;
import com.oracle.bmc.streaming.responses.PutMessagesResponse;
import com.oracle.bmc.util.internal.StringUtils;
import com.oracle.example.docrepo.cloudevents.OCIEventBinding;
import com.oracle.example.docrepo.cloudevents.ObjectStorageObjectEvent;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.fnproject.fn.api.RuntimeContext;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.xml.sax.SAXException;

import javax.json.*;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;


public class DocParser {
    private ObjectStorage objectStorageClient;
    private StreamAdminClient streamAdminClient;
    private final String outputBucketName;
    private ResourcePrincipalAuthenticationDetailsProvider provider;

    public DocParser(RuntimeContext ctx) {
        initOciClients();
        outputBucketName = reqEnv(ctx, "OUTPUT_BUCKET");
    }

    private String reqEnv(RuntimeContext ctx, String key) {
        return ctx.getConfigurationByKey(key).orElseThrow(() -> new RuntimeException("Missing required config " + key));

    }

    private void initOciClients() {
        System.out.println("Inside initOciClients");
        try {
            provider  = ResourcePrincipalAuthenticationDetailsProvider.builder().build();
            System.err.println("ResourcePrincipalAuthenticationDetailsProvider setup");
            objectStorageClient = ObjectStorageClient.builder().build(provider);
//            objectStorageClient.setRegion(Region.EU_FRANKFURT_1);
            System.out.println("ObjectStorage client setup");

            streamAdminClient = StreamAdminClient.builder().build(provider);
            System.out.println("Streaming clients setup");

        } catch (Exception ex) {
            System.err.println("Exception in FDK " + ex.getMessage());
            ex.printStackTrace();
            throw new RuntimeException("failed to init oci clients", ex);
        }
    }

    public GetObjectResponse readObject(String namespace, String bucketname, String filename) {
        try {
            GetObjectRequest getObjectRequest =
                    GetObjectRequest.builder()
                            .namespaceName(namespace)
                            .bucketName(bucketname)
                            .objectName(filename)
                            .build();
            GetObjectResponse getObjectResponse = objectStorageClient.getObject(getObjectRequest);
            getObjectResponse.getInputStream().close();

            return getObjectResponse;
        } catch (Exception e) {
            throw new RuntimeException("Could not read from os!" + e.getMessage());
        }
    }

    public JsonObject parseObject(GetObjectResponse objectResponse, String path) throws IOException, TikaException, SAXException {
        // Create a Tika instance with the default configuration
        Tika tika = new Tika();
        Metadata metadata = new Metadata();
        Reader reader = tika.parse(objectResponse.getInputStream(), metadata);
        JsonObjectBuilder jsonObjectBuilder = Json.createObjectBuilder();
        //getting metadata of the document
        String[] metadataNames = metadata.names();
        for (String name : metadataNames) {
            jsonObjectBuilder.add(name,metadata.get(name));
        }
        //getting the content of the document
        BufferedReader bufferedReader = new BufferedReader(reader);
        StringBuilder stringBuilder = new StringBuilder();
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            stringBuilder.append(line);
        }
        String content = stringBuilder.toString();
        jsonObjectBuilder.add("content",content);
        jsonObjectBuilder.add("md5",objectResponse.getContentMd5());
        jsonObjectBuilder.add("path",path);
        JsonObject jsonObject = jsonObjectBuilder.build();

        return jsonObject;
    }

    private void writeObject(JsonObject jsonObject, String namespace, String bucketName, String objectName) {
        try {
            String jsonString = jsonObject.toString();
            System.err.println("JSON: " + jsonString);
            PutObjectRequest putObjectRequest =
                    PutObjectRequest.builder()
                            .namespaceName(namespace)
                            .bucketName(bucketName)
                            .objectName(objectName)
                            .contentType("application/octet-stream")
                            .putObjectBody(new ByteArrayInputStream(jsonString.getBytes()))
                            .build();

            PutObjectResponse putObjectResponse = objectStorageClient.putObject(putObjectRequest);
            System.err.println("New object md5: " + putObjectResponse.getOpcContentMd5());
        } catch (Exception e) {
            throw new RuntimeException("Failed to write to os " + e);
        }
    }

    private void streamObject(JsonObject jsonObject,String objectName, String compartmentId){
        try {
            String jsonString = jsonObject.toString();
            System.err.println("JSON: " + jsonString);
            String streamName = "docrepo";
            Stream stream;
            ListStreamsRequest listRequest = ListStreamsRequest.builder().compartmentId(compartmentId)
                    .lifecycleState(Stream.LifecycleState.Active).name(streamName).build();
            ListStreamsResponse listResponse = streamAdminClient.listStreams(listRequest);
            if (!listResponse.getItems().isEmpty()) {
                // if we find an active stream with the correct name, we'll use it.
                System.out.println(String.format("An active stream named %s was found.", streamName));
                String streamId = listResponse.getItems().get(0).getId();
                GetStreamRequest getStreamRequest =
                    GetStreamRequest.builder()
                            .streamId(streamId)
                            .build();
                GetStreamResponse getStreamResponse = streamAdminClient.getStream(getStreamRequest);
                stream = getStreamResponse.getStream();
                StreamClient streamClient =
                        StreamClient.builder()
                                .endpoint(stream.getMessagesEndpoint()) //"https://cell-1.streaming.eu-frankfurt-1.oci.oraclecloud.com"
                                .build(provider);

                List<PutMessagesDetailsEntry> messages = new ArrayList<>();
                messages.add(PutMessagesDetailsEntry.builder()
                        .key(objectName.getBytes(UTF_8))
                        .value(jsonString.getBytes(UTF_8))
                        .build());

                PutMessagesDetails messagesDetails =
                    PutMessagesDetails.builder()
                            .messages(messages)
                            .build();
                PutMessagesRequest putRequest = PutMessagesRequest.builder().streamId(streamId)
                        .putMessagesDetails(messagesDetails).build();

                PutMessagesResponse putResponse = streamClient.putMessages(putRequest);

                // the putResponse can contain some useful metadata for handling failures
                for (PutMessagesResultEntry entry : putResponse.getPutMessagesResult().getEntries()) {
                    if (StringUtils.isNotBlank(entry.getError())) {
                        System.out.println(String.format("Error(%s): %s", entry.getError(), entry.getErrorMessage()));
                    } else {
                        System.out.println(String.format("Published message to partition %s, offset %s.", entry.getPartition(),
                                entry.getOffset()));
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to write to streaming " + e);
        }
    }

    public String handleRequest(@InputBinding(coercion = OCIEventBinding.class) ObjectStorageObjectEvent event) {
        System.err.println("Got a new event: " + event.toString());
        try {
            String namespace = event.additionalDetails.namespace;
            String bucketName = event.additionalDetails.bucketName;
            String resourceName = event.resourceName;
            String compartmentId = event.compartmentId;

            GetObjectResponse getObjectResponse = readObject(namespace,bucketName,resourceName);
            String path = "https://objectstorage.eu-frankfurt-1.oraclecloud.com/n/"+namespace+"/b/"+bucketName+"/o/"+resourceName;

            JsonObject jsondoc = parseObject(getObjectResponse, path);

            writeObject(jsondoc, namespace, outputBucketName, resourceName+".json");
            streamObject(jsondoc, resourceName, compartmentId);

            return jsondoc.toString(); //"ok";
        } catch (Exception ex) {
            System.err.println("Exception in FDK " + ex.getMessage());
            ex.printStackTrace();
            return "oops";
        }
    }

}