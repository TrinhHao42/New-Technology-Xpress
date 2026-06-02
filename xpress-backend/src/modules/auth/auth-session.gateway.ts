import {
  BadRequestException,
  Logger,
  UnauthorizedException,
} from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';
import {
  ConnectedSocket,
  OnGatewayConnection,
  WebSocketGateway,
  WebSocketServer,
} from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';

interface JwtPayload {
  sub: string;
}

@WebSocketGateway({
  namespace: '/auth-sessions',
})
export class AuthSessionGateway implements OnGatewayConnection {
  @WebSocketServer()
  server!: Server;

  private readonly logger = new Logger(AuthSessionGateway.name);

  constructor(private readonly jwtService: JwtService) {}

  handleConnection(@ConnectedSocket() client: Socket): void {
    try {
      const token = this.extractToken(client);
      const payload = this.jwtService.verify<JwtPayload>(token);
      client.join(this.toUserRoom(payload.sub));
    } catch (error) {
      this.logger.warn(`Socket auth failed: ${String(error)}`);
      client.disconnect(true);
    }
  }

  notifyNewLogin(
    userId: string,
    payload: {
      sessionId: string;
      deviceName?: string;
      ipAddress?: string;
      loggedInAt: string;
    },
  ): void {
    this.server
      .to(this.toUserRoom(userId))
      .emit('auth.session.new-login', payload);
  }

  notifySessionRevoked(
    userId: string,
    payload: {
      sessionId: string;
      revokedAt: string;
    },
  ): void {
    this.server
      .to(this.toUserRoom(userId))
      .emit('auth.session.revoked', payload);
  }

  private toUserRoom(userId: string): string {
    return `auth-user:${userId}`;
  }

  private extractToken(client: Socket): string {
    const rawToken = client.handshake.auth?.token;
    if (typeof rawToken !== 'string' || rawToken.trim().length === 0) {
      throw new UnauthorizedException('Socket token is required');
    }

    const normalized = rawToken.startsWith('Bearer ')
      ? rawToken.slice('Bearer '.length)
      : rawToken;

    if (!normalized) {
      throw new BadRequestException('Socket token is invalid');
    }

    return normalized;
  }
}
