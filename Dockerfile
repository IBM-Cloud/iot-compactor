FROM python:3

WORKDIR /usr/src/app

RUN pip install --no-cache-dir --upgrade --force-reinstall ibmcloudsql

COPY compact.py .

CMD [ "python", "./compact.py" ]
