type QueueEventType =
  | "pedido.criado"
  | "pagamento.processando"
  | "pagamento.aprovado"
  | "pagamento.recusado"
  | "pedido.atualizado";

type QueueMessageType<T = unknown> = {
  id: string;
  event: QueueEventType;
  data: T;
  createdAt: string;
};

export type { QueueEventType };
export default QueueMessageType;
