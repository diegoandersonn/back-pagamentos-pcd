import PedidoType from "./pedidoType";

type MensagemPedidoType = {
  id: string;
  pedido: PedidoType;
  data: Date;
};

export default MensagemPedidoType;
