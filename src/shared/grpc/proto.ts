import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";

const PROTO_PATH = process.env.PROTO_PATH ?? "/proto/pcd.proto";

type ServiceDefinition = grpc.ServiceDefinition<grpc.UntypedServiceImplementation>;

export type LoadedProto = {
  pcd: {
    PedidosService: { service: ServiceDefinition };
    PagamentosService: { service: ServiceDefinition };
    FilaService: { service: ServiceDefinition };
  };
};

export type EmptyRequest = Record<string, never>;

export type RpcCallback<T> = (error: grpc.ServiceError | null, response?: T) => void;

export function loadProto(): LoadedProto {
  const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });

  return grpc.loadPackageDefinition(packageDefinition) as unknown as LoadedProto;
}

export function bindGrpcServer(server: grpc.Server, port: string, serviceName: string): void {
  server.bindAsync(`0.0.0.0:${port}`, grpc.ServerCredentials.createInsecure(), (error) => {
    if (error) {
      throw error;
    }

    console.log(`${serviceName} gRPC rodando em 0.0.0.0:${port}`);
  });
}
