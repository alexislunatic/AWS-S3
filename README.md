# Subiendo archivos a S3

Repositorio del articulo original en [bigdateros.com](https://www.bigdateros.com/blog/index.php?entryid=4) 



## Uso
#### Ambiente
```python
# Configurar el ambiente, esto debe hacerse con aws cli
session = boto3.Session(profile_name='aadev')

```
#### Inicialización

```python
# ejecutar el terraform
"""Atributos:
		file (string) ruta del objeto a subir
		key (string) nombre del objeto
		bucket (string) bucket destino donde se subirán los archivos
		chunk_size (int) partes en MB que se subiran los archivos(aplica para multipart_upload)
		processes (int) numero de proceso en paralelo para la carga de archivos
"""
obj = S3Uploader(r'....ruta\video2.mp4', "video2.mp4", "s3-bucket-destino", 5, 3)
```

#### Carga de archivos multiparte
```python
 multipart_upload(obj.file, obj.key, obj.bucket, obj.chunk_size, obj.processes)
```

#### Carga de archivos con upload_file
```python
 obj.upload_to_aws(s3_client)
```

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## Autor
Alexis Alvarez
