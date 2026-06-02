import { Logger, UnauthorizedException } from '@nestjs/common';
import {
  OnGatewayConnection,
  OnGatewayDisconnect,
  WebSocketGateway,
} from '@nestjs/websockets';
import { JwtService } from '@nestjs/jwt';
import { Server, Socket } from 'socket.io';
import { FEED_EVENTS } from './constants/events';

interface JwtPayload {
  sub: string;
}

@WebSocketGateway({
  namespace: '/feed',
})
export class NewsFeedGateway implements OnGatewayConnection, OnGatewayDisconnect {
  private readonly logger = new Logger(NewsFeedGateway.name);
  private readonly userSockets = new Map<string, Set<string>>();
  private readonly socketUsers = new Map<string, string>();

  private server?: Server;

  constructor(private readonly jwtService: JwtService) {}

  afterInit(server: Server) {
    this.server = server;
  }

  handleConnection(client: Socket): void {
    try {
      const token = this.extractToken(client);
      const payload = this.jwtService.verify<JwtPayload>(token);
      const userId = payload.sub;

      client.data.userId = userId;
      this.socketUsers.set(client.id, userId);

      if (!this.userSockets.has(userId)) {
        this.userSockets.set(userId, new Set());
      }
      this.userSockets.get(userId)?.add(client.id);
    } catch (error) {
      this.logger.warn(`Feed socket auth failed: ${String(error)}`);
      client.disconnect(true);
    }
  }

  handleDisconnect(client: Socket): void {
    const userId = this.socketUsers.get(client.id);
    if (!userId) {
      return;
    }

    this.socketUsers.delete(client.id);
    const sockets = this.userSockets.get(userId);
    if (!sockets) {
      return;
    }

    sockets.delete(client.id);
    if (sockets.size === 0) {
      this.userSockets.delete(userId);
    }
  }

  emitPostCreated(payload: unknown): void {
    this.server?.emit(FEED_EVENTS.POST_CREATED, payload);
  }

  emitPostUpdated(payload: unknown): void {
    this.server?.emit(FEED_EVENTS.POST_UPDATED, payload);
  }

  emitPostDeleted(payload: unknown): void {
    this.server?.emit(FEED_EVENTS.POST_DELETED, payload);
  }

  emitReactionUpdated(payload: unknown): void {
    this.server?.emit(FEED_EVENTS.REACTION_UPDATED, payload);
  }

  emitCommentCreated(payload: unknown): void {
    this.server?.emit(FEED_EVENTS.COMMENT_CREATED, payload);
  }

  emitCommentDeleted(payload: unknown): void {
    this.server?.emit(FEED_EVENTS.COMMENT_DELETED, payload);
  }

  private extractToken(client: Socket): string {
    const authToken =
      typeof client.handshake.auth?.token === 'string'
        ? client.handshake.auth.token
        : '';

    const headerToken =
      typeof client.handshake.headers.authorization === 'string'
        ? client.handshake.headers.authorization.replace(/^Bearer\s+/i, '')
        : '';

    const token = authToken || headerToken;
    if (!token) {
      throw new UnauthorizedException('Unauthorized');
    }

    return token;
  }
}
