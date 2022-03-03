```bash
# Crear y activar ambiente de conda:
conda create --name declara python=3
conda activate declara

# Instalar dependencias de python:
conda install elasticsearch 
conda install -c conda-forge python-dotenv
conda install pymongo

# Editar .env y asignar variables de entorno:
nano .env 

# Ejecutar script para actualizar el índice:
python mongo2elastic.py
```
