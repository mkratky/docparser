# docparser
OCI Function triggered by OCI Events when a new object is created on Object Storage. The funciton extracts metadata and text from various document types using Apache Tika and writes the output as JSON to OCI Streaming, Object Storage bucket and OpenSearch.

# Steps to deploy
## Step 1: Create the Function Application
In the OCI console Go the menu
  #### Developer Services
    Functions / Application
  Check that you are in the right compartment 
  #### Click Create Application
    Name: e.g. docs-application
    VCN: <your existing VCN>
    Subnet: <your existing private subnet>
  #### Click Create
  
## Step 2: Build and deploy the Function in the Cloud Shell
  #### Start the cloud Shell (notice that you can upload and download files from it if needed)
  #### Run following commands in the shell:
    fn list context
    fn list contextfn use context eu-frankfurt-1
    fn update context oracle.compartment-id <your compartment ocid>
    fn update context registry fra.ocir.io/xxxxxx/docapp   #xxxxxx is your tenancy namespace
    docker login -u 'xxxxx/oracleidentitycloudservice/name@domain.com' fra.ocir.io #You will need to generate a token under your OCI user to log to the container registry.  
    git clone https://github.com/mkratky/docparser.git
    cd docparser
    fn -v deploy --app docs-application
    
## Step 3: Configure the Function variables
After building the code, it will create a function docparser in the doc-application
In the OCI console Go the menu
  #### Developer Services
    Functions / Application
  ####  Go back to the docs-application
  ####  Click Functions
  ####  Click the name of the function docparser
  ####  Click Configuration
      Add Key: OUTPUT_BUCKET , Value: <name of your existing Object Storage bucket e.g. docs-extract>
      Add Key: STREAM_NAME , Value: <e.g. docsextract , if the stream name doesn't exist it will get created>
      Add Key: SEARCH_ENDPOINT , Value: <your OpenSearch API endpoint>
   
## Step 4: Create an Object Storage Bucket   
Object storage will be used to contain the documents to index.
In OCI console Go the menu
  #### Storage
    Object Storage / Bucket
   Choose the right compartment 
   #### Click Create Bucket
    Bucket name: e.g. docs-upload
    Check: Emit Object Events
   #### Click Create
    
## Step 5: Create an Event Rule
In OCI console Go the menu
  #### Observability & Management
    Event Rules
  Check that you are in the right compartment
  #### Click Create Rule
      Display Name: docs-upload-rule
      Add the Rules Condition:
        Condition: Event Type
        Service: Object Storage
        Event Type: Object - Create, Object - Update
        Add Another Condition
        Condition: Attribute
        Attribute Name: bucketName
        Attribute value: docs-upload (Then press enter)
      In the actions:
        Action Type: Functions
        Function Compartment: <your compartment name>
        Function Application: docs-application
        Fuction: docparser
   #### Click Create Rule
    
## Step 6: Create a Dynamic group and Policies   
The Dynamic Group will allow to give rights to the function to read the Object Storage.
In OCI console Go the menu
  #### Identity & Security
    Dynamic Groups
   #### Click Create Dynamic Group
      Name: docs-fn-dyngroup
      Description: docs-fn-dyngroup
      Rule: ALL {resource.type = 'fnfunc', resource.compartment.id = '##COMPARTMENT_OCID##'} 
      where you need to replace the value ##COMPARTMENT_OCID## with your compartment ocid
   #### Click Create
  
 In OCI console Go the menu
   #### Identity & Security
    Policies
      Check that you are in the right compartment
   #### Click Create Policy
      Name: docs-fn-policy
      Description: docs-fn-policy
      Choose: Show manual editor
      Copy paste the policy: Allow dynamic-group docs-fn-dyngroup to manage objects in compartment ##COMPARTMENT_NAME##
      where you need to replace the value ##COMPARTMENT_NAME## with your compartment name
      
## Step 7: Running the application  
To run the application upload your document(s) you wish to index to the Object storage bucket
In OCI console Go the menu
  #### Storage
    Object Storage / Bucket
   Choose the right compartment 
   #### Click Bucket name: e.g. docs-upload 
   #### Click Upload
    Choose Files from your Computer (Drop files here or select files)
   #### Click Upload
    
    
      
      
