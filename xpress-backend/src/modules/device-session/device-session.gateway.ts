import {
  forwardRef,
  Inject,
  Injectable,
  Logger,
  UnauthorizedException,
} from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';
import {
  ConnectedSocket,
  OnGatewayConnection,
  OnGatewayDisconnect,
  WebSocketGateway,
  WebSocketServer,
} from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';
import {
  DEVICE_SESSION_EVENTS,
  DEVICE_SESSION_NAMESPACE,
  ForceLogoutPayload,
} from './device-session.events';
import { DeviceSessionService } from './device-session.service';

interface JwtPayload {
  sub: string;
  sid?: string;
}

interface DeviceSessionSocketData {
  userId: string;
  sessionId: string;
}

@Injectable()
@WebSocketGateway({
  namespace: DEVICE_SESSION_NAMESPACE,
})
export class DeviceSessionGateway
  implements OnGatewayConnection, OnGatewayDisconnect
{
  @WebSocketServer()
  server!: Server;

  private readonly logger = new Logger(DeviceSessionGateway.name);

  constructor(
    private readonly jwtService: JwtService,
    @Inject(forwardRef(() => DeviceSessionService))
    private readonly deviceSessionService: DeviceSessionService,
  ) {}

  async handleConnection(@ConnectedSocket() client: Socket): Promise<void> {
    try {
      const token = this.extractToken(client);
      const payload = this.jwtService.verify<JwtPayload>(token);
      if (!payload.sub || !payload.sid) {
        throw new UnauthorizedException('Invalid device-session token');
      }

      const stillValid = await this.deviceSessionService.isSessionValid(
        payload.sub,
        payload.sid,
      );
      if (!stillValid) {
        client.emit(DEVICE_SESSION_EVENTS.FORCE_LOGOUT, {
          reason: 'NEW_DEVICE_LOGIN',
          at: new Date().toISOString(),
        } satisfies ForceLogoutPayload);
        client.disconnect(true);
        return;
      }

      await this.deviceSessionService.registerSocket(
        payload.sub,
        payload.sid,
        client.id,
      );

      (client.data as DeviceSessionSocketData) = {
        userId: payload.sub,
        sessionId: payload.sid,
      };
      client.emit(DEVICE_SESSION_EVENTS.BIND_ACK, { ok: true });
    } catch (error) {
      this.logger.warn(`Device-session socket auth failed: ${String(error)}`);
      client.disconnect(true);
    }
  }

  async handleDisconnect(@ConnectedSocket() client: Socket): Promise<void> {
    const data = client.data as DeviceSessionSocketData | undefined;
    if (!data?.userId) {
      return;
    }
    await this.deviceSessionService.clearSocket(data.userId, client.id);
  }

  forceLogout(socketId: string, payload: ForceLogoutPayload): void {
    if (!this.server) {
      return;
    }
    // `this.server` is the namespace (/device-sessions) because of the
    // @WebSocketGateway({ namespace }) decorator. Each socket auto-joins a
    // room equal to its own socket id, so targeting `to(socketId)` is the
    // stable, public API for single-socket emits.
    this.server.to(socketId).emit(DEVICE_SESSION_EVENTS.FORCE_LOGOUT, payload);
    // Give the client a tick to process the event before tearing down.
    setTimeout(() => {
      this.server.in(socketId).disconnectSockets(true);
    }, 250);
  }

  private extractToken(client: Socket): string {
    const raw = client.handshake.auth?.token;
    if (typeof raw !== 'string' || raw.trim().length === 0) {
      throw new UnauthorizedException('Missing device-session token');
    }
    return raw.startsWith('Bearer ') ? raw.slice('Bearer '.length) : raw;
  }
}
