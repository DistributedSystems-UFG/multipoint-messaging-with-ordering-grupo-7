FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir grpcio==1.62.1 grpcio-tools==1.62.1
COPY proto/ /app/proto/
RUN python -m grpc_tools.protoc \
      -I/app/proto \
      --python_out=/app \
      --grpc_python_out=/app \
      /app/proto/naming.proto
COPY naming_service/server.py /app/naming_service/server.py
ENV PORT=50050
EXPOSE 50050
CMD ["python", "naming_service/server.py"]
