# PCD Pagamentos Service

Microservico responsavel por processar pagamentos a partir de eventos de pedido criado.

## Scripts

- `npm run dev`: inicia em desenvolvimento
- `npm run build`: compila TypeScript
- `npm start`: executa o build

## Portas padrao

- gRPC: `50052`
- HTTP/Socket.IO: `3002`

## Variaveis

- `PAGAMENTOS_GRPC_PORT`: porta gRPC
- `PAGAMENTOS_HTTP_PORT`: porta HTTP/Socket.IO
- `RABBITMQ_URL`: URL do RabbitMQ
- `SERVICE_NAME`: nome da fila exclusiva do servico
