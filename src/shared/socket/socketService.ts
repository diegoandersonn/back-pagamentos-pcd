import type { Server as HttpServer } from "node:http";
import { Server } from "socket.io";

let io: Server | undefined;

export function initializeSocket(server: HttpServer): Server {
  io = new Server(server, {
    cors: {
      origin: ["http://localhost:3000", "http://localhost:3002"],
    },
  });

  io.on("connection", (socket) => {
    console.log("Cliente conectado ao Socket.IO:", socket.id);
  });

  return io;
}

export function emitSocketEvent<T>(event: string, payload: T): void {
  io?.emit(event, payload);
}
