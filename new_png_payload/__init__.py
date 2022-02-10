import logging

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

from PIL import Image
import pytesseract
import io 

import azure.functions as func

def store_content(output_path : str, document_content : str):
    connect_str = 'DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=unique54storagename;AccountKey=FffG8tJM8JJ2qP0iBgXETCSrMC6+6XQ25EgtedQqrabwx+fHjy7pILIbEFBtOnbrXX9fJddf5OsECtXkdprE/A=='
    container_name = 'companies-doc-images'

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service_client.get_blob_client(container=container_name,  blob=output_path)

    blob_client.upload_blob(document_content)
    logging.info(f"Document OCR results storing complete")

def png_ocr(image_data):
    #Required for local only
    #pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

    text = ""
    byteImgIO = io.BytesIO(image_data)
    byteImgIO.seek(0)
    
    image = Image.open(byteImgIO)
    
    text = pytesseract.image_to_string(image)
    
    return text

def parse_complete_pngs(complete_file_path : str):
   
    connect_str = 'DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=unique54storagename;AccountKey=FffG8tJM8JJ2qP0iBgXETCSrMC6+6XQ25EgtedQqrabwx+fHjy7pILIbEFBtOnbrXX9fJddf5OsECtXkdprE/A=='

    blob = BlobClient.from_connection_string(conn_str=connect_str, container_name="companies-doc-images", blob_name=complete_file_path)
    blob_data = blob.download_blob()
    png_locations = blob_data.readall().decode("utf-8").splitlines()
    
    document_content = ""

    for png_location in png_locations:
        logging.info(f"Document OCR: {png_location}")
        blob = BlobClient.from_connection_string(conn_str=connect_str, container_name="companies-doc-images", blob_name=png_location)
        blob_data = blob.download_blob()
                
        png_content = png_ocr(blob_data.readall())

        document_content += png_content

    logging.info(f"Document OCR complete")

    return document_content


def is_complete_document(file_name : str):
    suffix = "complete.txt"
    
    return file_name.endswith(suffix)

def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")

    if is_complete_document(myblob.name):
        content = parse_complete_pngs(myblob.name[21:])
        content_blob_name = myblob.name[21:-12] + 'document_content.txt'
        store_content(content_blob_name, content)
    
