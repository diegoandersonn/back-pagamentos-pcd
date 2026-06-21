import { randomUUID } from "node:crypto";
import { emitSocketEvent } from "../../shared/socket/socketService";
import AsyncMutex from "../../shared/sync/asyncMutex";
import type MensagemPedidoType from "../../types/mensagemPedidoType";
import pagamentos from "./pagamentosStore";
import type PagamentoType from "./pagamentosTypes";

const pagamentosMutex = new AsyncMutex();

function clonePagamento(pagamento: PagamentoType): PagamentoType {
  return { ...pagamento };
}

export async function listPagamentos(): Promise<PagamentoType[]> {
  return pagamentosMutex.runExclusive(() => pagamentos.map(clonePagamento));
}

export async function iniciarPagamento(mensagemPedido: MensagemPedidoType): Promise<PagamentoType> {
  const pagamento: PagamentoType = {
    id: randomUUID(),
    pedidoId: mensagemPedido.pedido.id,
    cliente: mensagemPedido.pedido.cliente,
    item: mensagemPedido.pedido.item,
    status: "Processando",
    criadoEm: new Date().toISOString(),
  };

  const savedPayment = await pagamentosMutex.runExclusive(() => {
    pagamentos.push(pagamento);
    return clonePagamento(pagamento);
  });

  emitSocketEvent("pagamento:processando", savedPayment);

  return savedPayment;
}

export async function finalizarPagamento(id: string): Promise<PagamentoType | undefined> {
  const finishedPayment = await pagamentosMutex.runExclusive(() => {
    const pagamento = pagamentos.find((item) => item.id === id);

    if (!pagamento) {
      return undefined;
    }

    pagamento.status = Math.random() >= 0.25 ? "Aprovado" : "Recusado";
    pagamento.processadoEm = new Date().toISOString();
    return clonePagamento(pagamento);
  });

  if (finishedPayment) {
    emitSocketEvent(
      finishedPayment.status === "Aprovado" ? "pagamento:aprovado" : "pagamento:recusado",
      finishedPayment,
    );
  }

  return finishedPayment;
}
