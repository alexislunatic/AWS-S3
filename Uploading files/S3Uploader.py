import re
import multiprocessing
import json
import boto3
session = boto3.Session(profile_name='aadev')
# s3_client = session.client('s3')


def start_upload(bucket, key):
    s3_client = session.client('s3')
    response = s3_client.create_multipart_upload(
        Bucket=bucket,
        Key=key
    )
    return response['UploadId']


def end_upload(bucket, key, upload_id, finished_parts):
    s3_client = session.client('s3')

    response = s3_client.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        MultipartUpload={
            'Parts': finished_parts
        },
        UploadId=upload_id
    )
    return response

    # @staticmethod


def add_part(proc_queue, body, bucket, key, part_number, upload_id):
    # s3_client = self.session.client('s3')
    s3_client = session.client('s3')

    response = s3_client.upload_part(
        Body=body,
        Bucket=bucket,
        Key=key,
        PartNumber=part_number,
        UploadId=upload_id
    )
    print(f"Parte: {part_number}, ETag: {response['ETag']}")
    proc_queue.put({'PartNumber': part_number,
                    'ETag': response['ETag']})
    return


def multipart_upload(file, key, bucket, chunk_size, processes):

    upload_id = start_upload(bucket, key)
    print(f'Iniciando subida: {upload_id}')
    file_upload = open(file, 'rb')
    part_procs = []
    proc_queue = multiprocessing.Queue()
    queue_returns = []
    chunk_size = (chunk_size * 1024) * 1024
    part_num = 1
    chunk = file_upload.read(chunk_size)

    while len(chunk) > 0:
        proc = multiprocessing.Process(target=add_part, args=(
            proc_queue, chunk, bucket, key, part_num, upload_id))
        part_procs.append(proc)
        part_num += 1
        chunk = file_upload.read(chunk_size)

    part_procs = [part_procs[i * processes:(i + 1) * processes]
                  for i in range((len(part_procs) + (processes - 1)) // processes)]

    for i in range(len(part_procs)):
        for p in part_procs[i]:
            p.start()
        for p in part_procs[i]:
            p.join()
        for p in part_procs[i]:
            queue_returns.append(proc_queue.get())

    queue_returns = sorted(queue_returns, key=lambda i: i['PartNumber'])
    response = end_upload(
        bucket, key, upload_id, queue_returns)
    print(json.dumps(response, sort_keys=True, indent=4))


class S3Uploader():

    def __init__(self,  file="", key="", bucket="", chunk_size=5, processes=1):
        """ Clase para subida de archivos usando multipart upload.

                Attributes:
                        file (string) ruta del objeto a subir
                        key (string) nombre del objeto
                        bucket (string) bucket destino donde se subirán los archivos
                        chunk_size (int) partes en MB que se subiran los archivos(aplica para multipart_upload)
                        processes (int) numero de proceso en paralelo para la carga de archivos
                        """
        self.file = file
        self.key = self._validate_key(key, file)
        self.bucket = bucket
        self.chunk_size = self._validate_chunk_size(chunk_size)
        self.processes = processes

    def _validate_key(self, key, file):
        if key in [None, '']:
            key = file
        return key

    def _validate_chunk_size(self, chunk_size):

        if (chunk_size >= 5 and chunk_size <= 100) == False:
            raise ValueError("Chunk size in MB, must be > 5MiB")
        return chunk_size

    def upload_to_aws(self, s3_client):
        try:
            s3_client.upload_file(self.file, self.bucket, self.key)
            print("Subida exitosa")
            return True
        except FileNotFoundError:
            print("Archivo no encontrado")
            return False
        except Exception as e:
            print(str(e))
            return False

    def put_object(self, s3_client):
        try:
            # Send the file
            with open(self.file, 'rb') as fd:
                response = s3_client.put_object(
                    Bucket=self.bucket,
                    Key=self.key,
                    Body=fd
                )
            print(json.dumps(response, sort_keys=True, indent=4))

            print("Put Object exitoso")
            return True
        except FileNotFoundError:
            print("Archivo no encontrado")
            return False
        except Exception as e:
            print(str(e))
            return False


if __name__ == '__main__':
    s3_client = session.client('s3')
    obj = S3Uploader(r'ruta\video2.mp4',
                     "video2.mp4", "antamina-aa-dev-test123", 5, 3)
    #Carga de archivos multiparte
    multipart_upload(obj.file, obj.key, obj.bucket,
                     obj.chunk_size, obj.processes)
    #Carga de archivos con upload_file                      
    obj.upload_to_aws(s3_client)
    print(obj)
