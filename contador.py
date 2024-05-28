import sys
from pyspark import SparkContext, SparkConf
import csv
from tempfile import NamedTemporaryFile
from google.cloud import storage

if __name__ == "__main__":
    sc = SparkContext("local", "PySpark Exemplo - Desafio Dataproc")
    
    words = sc.textFile("gs://{SEU_BUCKET}/livro.txt").flatMap(lambda line: line.split(" "))

    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda a: a[1], ascending=False)
         
    output = wordCounts.collect()

    output_gcs_path = "gs://{SEU_BUCKET}/resultados/resultado.csv"
    
    with NamedTemporaryFile(mode='w', newline='', delete=False) as temp_csvfile:
        csvwriter = csv.writer(temp_csvfile)
        csvwriter.writerow(["Word", "Count"]) 
        csvwriter.writerows(output)
        temp_csvfile_path = temp_csvfile.name
    
    client = storage.Client()
    bucket_name = '{SEU_BUCKET}'
    destination_blob_name = 'resultados/resultado.csv'
    
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(temp_csvfile_path)
    
    sc.stop()
