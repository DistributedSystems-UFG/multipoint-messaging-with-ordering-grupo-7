FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir grpcio==1.62.1 grpcio-tools==1.62.1
COPY proto/ /app/proto/
RUN python -m grpc_tools.protoc \
      -I/app/proto \
      --python_out=/app \
      --grpc_python_out=/app \
      /app/proto/naming.proto \
      /app/proto/peer.proto
COPY peer/peer.py /app/peer/peer.py
ENV PEER_PORT=50070
EXPOSE 50070
CMD ["python", "peer/peer.py"]
