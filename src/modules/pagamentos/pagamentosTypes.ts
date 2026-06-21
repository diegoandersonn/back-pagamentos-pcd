type PagamentoType = {
  id: string;
  pedidoId: string;
  cliente: string;
  item: string;
  status: "Processando" | "Aprovado" | "Recusado";
  criadoEm: string;
  processadoEm?: string;
};

export default PagamentoType;
