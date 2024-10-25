FROM python:3.7-alpine

WORKDIR .

RUN apk add --no-cache gcc musl-dev linux-headers geos libc-dev postgresql-dev
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

EXPOSE 5000
#EXPOSE 5001

COPY . .

RUN python -m grpc_tools.protoc -I./ --python_out=./ --grpc_python_out=./ person.proto
CMD ["flask", "run", "--host", "0.0.0.0"]
#CMD ["python", "-c", "from app import serve; serve()"]
#CMD ["python", "-c", "from app import create_app; app = create_app(); app.run(host='0.0.0.0', port=5000)"]
#CMD ["python", "-m", "app"]
#CMD ["sh", "-c", "python -m flask run --host=0.0.0.0 & python -m grpc_server & wait"]
