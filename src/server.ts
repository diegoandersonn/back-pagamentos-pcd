import http from "node:http";
import { startPagamentosGrpcServer } from "./grpcPagamentosServer";
import { registerPagamentosConsumers } from "./modules/pagamentos/pagamentosConsumer";
import { connectQueue } from "./shared/queue/rabbitmqService";
import { initializeSocket } from "./shared/socket/socketService";

const HTTP_PORT = Number(process.env.PAGAMENTOS_HTTP_PORT ?? process.env.HTTP_PORT ?? 3002);

const server = http.createServer();
initializeSocket(server);
registerPagamentosConsumers();

async function iniciarServidor(): Promise<void> {
  try {
    await connectQueue();
    startPagamentosGrpcServer();

    server.listen(HTTP_PORT, () => {
      console.log(`Pagamentos HTTP/Socket.IO rodando em http://localhost:${HTTP_PORT}`);
    });
  } catch (error) {
    console.error("Falha ao iniciar o servico de pagamentos:", error);
    process.exit(1);
  }
}

void iniciarServidor();
