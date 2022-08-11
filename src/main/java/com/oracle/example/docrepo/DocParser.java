package com.oracle.example.docrepo;

import com.fnproject.fn.api.InputBinding;
import com.oracle.bmc.Region;
import com.oracle.bmc.auth.AuthenticationDetailsProvider;
import com.oracle.bmc.auth.ResourcePrincipalAuthenticationDetailsProvider;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.requests.PutObjectRequest;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;
import com.oracle.bmc.objectstorage.responses.PutObjectResponse;

import java.io.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import com.oracle.bmc.responses.AsyncHandler;
import com.oracle.bmc.objectstorage.ObjectStorageAsyncClient;

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
import java.util.ArrayList;
import java.util.List;
import java.nio.charset.StandardCharsets;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import static java.nio.charset.StandardCharsets.UTF_8;


public class DocParser {
    private ObjectStorage objectStorageClient;
    private ObjectStorageAsyncClient objectStorageAsyncClient;
    private StreamAdminClient streamAdminClient;
    private final String outputBucketName;
    private final String openSearchEndpoint;

    private final String streamName;

    private ResourcePrincipalAuthenticationDetailsProvider provider;

    public DocParser(RuntimeContext ctx) {
        initOciClients();
        outputBucketName = reqEnv(ctx, "OUTPUT_BUCKET");
        openSearchEndpoint = reqEnv(ctx, "SEARCH_ENDPOINT");
        streamName = reqEnv(ctx, "STREAM_NAME");

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
            objectStorageAsyncClient = ObjectStorageAsyncClient.builder().build(provider);

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
        objectResponse.getInputStream().close();
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
        bufferedReader.close();
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
            ByteArrayInputStream baos = new ByteArrayInputStream(jsonString.getBytes(StandardCharsets.UTF_8));
            PutObjectRequest putObjectRequest =
                    PutObjectRequest.builder()
                            .namespaceName(namespace)
                            .bucketName(bucketName)
                            .objectName(objectName)
                            .contentType("application/json")
                            .putObjectBody(baos)
                            .contentLength((long) baos.available())
                            .build();

            PutObjectResponse putObjectResponse = objectStorageClient.putObject(putObjectRequest);
            System.err.println("New object md5: " + putObjectResponse.getOpcContentMd5());

//            ResponseHandler<PutObjectRequest, PutObjectResponse> putObjectHandler = new ResponseHandler<>();
//            Future<PutObjectResponse> putObjectResponseFuture = objectStorageAsyncClient.putObject(putObjectRequest, putObjectHandler);


 
        } catch (Exception e) {
            throw new RuntimeException("Failed to write to os " + e);
        }
    }

    private void streamObject(JsonObject jsonObject,String objectName, String compartmentId){
        try {
            String jsonString = jsonObject.toString();
            System.err.println("JSON: " + jsonString);
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

    private void indexObject(JsonObject jsonObject, String objectName) {
        try {
            String jsonString = jsonObject.toString();
            System.err.println("JSON: " + jsonString);

            CloseableHttpClient httpClient = HttpClientBuilder.create().build();
            HttpPost httpPost = new HttpPost(openSearchEndpoint + "/docrepo/document");
            ByteArrayEntity input = new ByteArrayEntity(jsonString.getBytes(StandardCharsets.UTF_8));
            input.setContentType("application/json");
            httpPost.setEntity(input);

            CloseableHttpResponse httpResponse = httpClient.execute(httpPost);
            System.out.println("POST Response Status:: " + httpResponse.getStatusLine().getStatusCode());
            BufferedReader br = new BufferedReader(new InputStreamReader((httpResponse.getEntity().getContent())));
            System.out.println("Output from Server .... \n");
            String output = "";
            while ((output = br.readLine()) != null) {
                System.out.println(output);
            }
            httpClient.close();
        } catch (Exception e) {
            throw new RuntimeException("Failed to write to index " + e);
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

            streamObject(jsondoc, resourceName, compartmentId);
            writeObject(jsondoc, namespace, outputBucketName, resourceName+".json");
            indexObject(jsondoc, resourceName);

            return jsondoc.toString(); //"ok";
        } catch (Exception ex) {
            System.err.println("Exception in FDK " + ex.getMessage());
            ex.printStackTrace();
            return "oops";
        }
    }
    static class ResponseHandler<IN, OUT> implements AsyncHandler<IN, OUT> {
        private Throwable failed = null;
        private CountDownLatch latch = new CountDownLatch(1);

        private void waitForCompletion() throws Exception {
            latch.await();
            if (failed != null) {
                if (failed instanceof Exception) {
                    throw (Exception) failed;
                }
                throw (Error) failed;
            }
        }

        @Override
        public void onSuccess(IN request, OUT response) {
            if (response instanceof PutObjectResponse) {
                System.out.println(
                        "New object md5: " + ((PutObjectResponse) response).getOpcContentMd5());
            } else if (response instanceof GetObjectResponse) {
                System.out.println("Object md5: " + ((GetObjectResponse) response).getContentMd5());
            }
            latch.countDown();
        }

        @Override
        public void onError(IN request, Throwable error) {
            failed = error;
            latch.countDown();
        }
    }


}