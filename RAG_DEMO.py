#Create a Snowpark based function to extract text from PDFs
import pandas as pd
from PyPDF2 import PDfFileReader
from snowflake.snowpark.files import SnowflakeFile
from io import BytesIO
from snowflake.snowpark.types import StringType, StructField, StructType
from langchain.text_splitter import RecursiveCharacterTextSplitter
import json

#Establish your connection to Snowflake
#Read Credentials
with open(r'directory/folder_name') as f:
    connection_parameters = json.load(f)

#Connect to a Snowflake Session
session = Session.builder.configs(connection_parameters).create()

#Create a function to extract text from a PDF and store it as characters in a table
session = Session.builder.configs(connection_parameters).create()

#Create a function to extract a text from a PDF and store it as characters in a table
def readpdf(file_path):
    whole_text = ""
    with SnowflakeFile.open(file_path, 'rb') as file:
        f = BytesIO(file.readall())
        pdf_reader = PdfFileReader(f)
        whole_text=""
        for page in pdf_reader.pages:
            whole_text += page.extract_text()
    return whole_text

#Register the User Defined Function
session.udf.register(
    func = readpdf
    , return_type = StringType()
    , input_types = [StringType()]
    , is_permanent = True
    , name = 'SNOWPARK_UDF'
    , replace = True
    , packages = ['snowflake-snowpark-python', 'pypdf2']
    , stage_location = '@STAGE_NAME'
)

#A class for chunking text and returning a table via UDTF
class text_chunker:

    def process(self,text):        
        text_raw=[]
        text_raw.append(text) 
        
        text_splitter = RecursiveCharacterTextSplitter(
            separators = ["\n"], # Define an appropriate separator. New line is good typically!
            chunk_size = 1000, #Adjust this as you see fit
            chunk_overlap  = 50, #This let's text have some form of overlap. Useful for keeping chunks contextual
            length_function = len,
            add_start_index = True #Optional but useful if you'd like to feed the chunk before/after
        )
    
        chunks = text_splitter.create_documents(text_raw)
        df = pd.DataFrame(chunks, columns=['chunks','meta'])
        
        yield from df.itertuples(index=False, name=None)

#Register the UDTF - set the stage location

schema = StructType([
     StructField("chunk", StringType()),
    StructField("meta", StringType()),
 ])

session.udtf.register( 
    handler = text_chunker,
    output_schema= schema, 
    input_types = [StringType()] , 
    is_permanent = True , 
    name = 'CHUNK_TEXT' , 
    replace = True , 
    packages=['pandas','langchain'], stage_location = '@STAGE_NAME')
