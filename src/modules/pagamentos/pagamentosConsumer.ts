import { publishQueueMessage, subscribeToQueue } from "../../shared/queue/rabbitmqService";
import type QueueMessageType from "../../shared/types/queueMessageType";
import type MensagemPedidoType from "../../types/mensagemPedidoType";
import { finalizarPagamento, iniciarPagamento } from "./pagamentosService";
import type PagamentoType from "./pagamentosTypes";

export function registerPagamentosConsumers(): void {
  subscribeToQueue<MensagemPedidoType>("pedido.criado", async (message) => {
    console.log("Pedido recebido para pagamento:", message.data.pedido.id);

    const pagamento = await iniciarPagamento(message.data);

    await publishQueueMessage("pagamento.processando", pagamento);

    await delay(3000);

    const pagamentoFinalizado = await finalizarPagamento(pagamento.id);

    if (!pagamentoFinalizado) {
      console.warn("Pagamento nao encontrado para finalizar:", pagamento.id);
      return;
    }

    console.log("Pagamento finalizado:", pagamentoFinalizado.id, pagamentoFinalizado.status);

    await publishQueueMessage(
      pagamentoFinalizado.status === "Aprovado" ? "pagamento.aprovado" : "pagamento.recusado",
      pagamentoFinalizado,
    );
  });

  subscribeToQueue<PagamentoType>("pagamento.processando", logPagamentoEvent);
}

function logPagamentoEvent(message: QueueMessageType<PagamentoType>): void {
  console.log("Pagamento em processamento:", message.data);
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}
