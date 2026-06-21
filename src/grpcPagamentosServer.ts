import * as grpc from "@grpc/grpc-js";
import { listPagamentos } from "./modules/pagamentos/pagamentosService";
import type PagamentoType from "./modules/pagamentos/pagamentosTypes";
import { bindGrpcServer, EmptyRequest, loadProto, RpcCallback } from "./shared/grpc/proto";

const GRPC_PORT = process.env.PAGAMENTOS_GRPC_PORT ?? process.env.GRPC_PORT ?? "50052";

type RpcPagamento = Omit<PagamentoType, "processadoEm"> & {
  processadoEm: string;
};

function toRpcPagamento(pagamento: PagamentoType): RpcPagamento {
  return {
    ...pagamento,
    processadoEm: pagamento.processadoEm ?? "",
  };
}

export function startPagamentosGrpcServer(): void {
  const proto = loadProto();
  const server = new grpc.Server();

  server.addService(proto.pcd.PagamentosService.service, {
    ListarPagamentos(
      _call: grpc.ServerUnaryCall<EmptyRequest, { pagamentos: RpcPagamento[] }>,
      callback: RpcCallback<{ pagamentos: RpcPagamento[] }>,
    ): void {
      void listPagamentos()
        .then((pagamentos) => callback(null, { pagamentos: pagamentos.map(toRpcPagamento) }))
        .catch((error) => callback(error as grpc.ServiceError));
    },
  });

  bindGrpcServer(server, GRPC_PORT, "PagamentosService");
}
