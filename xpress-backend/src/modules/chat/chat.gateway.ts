import {
  BadRequestException,
  Logger,
  UnauthorizedException,
} from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';
import {
  ConnectedSocket,
  MessageBody,
  OnGatewayInit,
  OnGatewayConnection,
  OnGatewayDisconnect,
  SubscribeMessage,
  WebSocketGateway,
} from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';
import { ChatService } from './chat.service';
import { CALL_EVENTS, CHAT_EVENTS } from './constants/events';
import { DeleteMessageDto } from './dto/delete-message.dto';
import { RecallMessageDto } from './dto/recall-message.dto';
import { ReplyMessageDto } from './dto/reply-message.dto';
import { SendGroupMessageDto } from './dto/send-group-message.dto';
import { SendMessageDto } from './dto/send-message.dto';
import type {
  CallAnswerDto,
  CallEndDto,
  CallIceDto,
  CallOfferDto,
} from './interfaces/chat-call.interface';
import type {
  GroupCallEndDto,
  GroupCallSignalDto,
  GroupCallStartDto,
  IncomingCallPayload,
  JwtPayload,
  ReadRoomDto,
  ReceiveDto,
  TypingDto,
} from './interfaces/chat-gateway.interface';
import { ChatGatewayTransportService } from './services/chat-gateway-transport.service';

@WebSocketGateway({
  namespace: '/chat',
})
export class ChatGateway
  implements OnGatewayInit, OnGatewayConnection, OnGatewayDisconnect
{
  private readonly logger = new Logger(ChatGateway.name);
  private readonly maxGroupCallParticipants = 5;
  private readonly activeGroupCallParticipants = new Map<string, Set<string>>();

  constructor(
    private readonly chatService: ChatService,
    private readonly jwtService: JwtService,
    private readonly transportService: ChatGatewayTransportService,
  ) {}

  afterInit(server: Server): void {
    this.transportService.setServer(server);
  }

  async handleConnection(client: Socket): Promise<void> {
    try {
      const token = this.extractToken(client);
      const payload = this.jwtService.verify<JwtPayload>(token);
      await this.transportService.handleConnection(client, payload.sub);

      const becameOnline = this.transportService.registerConnection(
        payload.sub,
        client.id,
      );
      if (becameOnline) {
        this.transportService.emitToUsers(
          [payload.sub],
          CHAT_EVENTS.PRESENCE,
          this.transportService.getPresence(payload.sub),
        );
      }
    } catch (error) {
      this.logger.warn(`Socket auth failed: ${String(error)}`);
      client.emit(CHAT_EVENTS.ERROR, { message: 'Unauthorized' });
      client.disconnect(true);
    }
  }

  handleDisconnect(client: Socket): void {
    const userId = client.data.userId as string | undefined;
    if (!userId) return;

    for (const [roomId, participants] of this.activeGroupCallParticipants.entries()) {
      participants.delete(userId);
      if (participants.size === 0) {
        this.activeGroupCallParticipants.delete(roomId);
      }
    }

    const becameOffline = this.transportService.handleDisconnect(
      userId,
      client.id,
    );
    if (becameOffline) {
      this.transportService.emitToUsers(
        [userId],
        CHAT_EVENTS.PRESENCE,
        this.transportService.getPresence(userId),
      );
    }
  }

  @SubscribeMessage(CHAT_EVENTS.SEND)
  async onSend(
    @ConnectedSocket() client: Socket,
    @MessageBody() dto: SendMessageDto,
  ): Promise<void> {
    const userId = this.getUserId(client);
    const message = await this.chatService.sendMessage(userId, dto);

    this.transportService.emitToUsers(
      [message.senderId, message.receiverId],
      CHAT_EVENTS.MESSAGE,
      message,
    );
  }

  @SubscribeMessage(CHAT_EVENTS.GROUP_SEND)
  async onGroupSend(
    @ConnectedSocket() client: Socket,
    @MessageBody() dto: SendGroupMessageDto,
  ): Promise<void> {
    const userId = this.getUserId(client);
    const message = await this.chatService.sendGroupMessage(userId, dto);

    this.transportService.emitToGroup(
      dto.roomId,
      CHAT_EVENTS.GROUP_MESSAGE,
      message,
    );
  }

  @SubscribeMessage(CHAT_EVENTS.REPLY)
  async onReply(
    @ConnectedSocket() client: Socket,
    @MessageBody() dto: ReplyMessageDto,
  ): Promise<void> {
    const userId = this.getUserId(client);
    const message = await this.chatService.replyMessage(userId, dto);

    this.transportService.emitToUsers(
      [message.senderId, message.receiverId],
      CHAT_EVENTS.MESSAGE,
      message,
    );
  }

  @SubscribeMessage(CHAT_EVENTS.DELETE)
  async onDelete(
    @ConnectedSocket() client: Socket,
    @MessageBody() dto: DeleteMessageDto,
  ): Promise<void> {
    const userId = this.getUserId(client);
    const message = await this.chatService.deleteMessage(userId, dto);

    const payload = {
      messageId: message.messageId,
      senderId: message.senderId,
      receiverId: message.receiverId,
      roomId: message.roomId ?? message.conversationId,
      roomType: message.roomType,
      isDeleted: true,
      updatedAt: message.updatedAt,
    };

    if (message.roomType === 'GROUP') {
      this.transportService.emitToGroup(
        message.roomId ?? message.conversationId,
        CHAT_EVENTS.DELETED,
        payload,
      );
      return;
    }

    this.transportService.emitToUsers(
      [message.senderId, message.receiverId],
      CHAT_EVENTS.DELETED,
      payload,
    );
  }

  @SubscribeMessage(CHAT_EVENTS.RECALL)
  async onRecall(
    @ConnectedSocket() client: Socket,
    @MessageBody() dto: RecallMessageDto,
  ): Promise<void> {
    const userId = this.getUserId(client);
    const message = await this.chatService.recallMessage(userId, dto);

    const payload = {
      messageId: message.messageId,
      senderId: message.senderId,
      receiverId: message.receiverId,
      roomId: message.roomId ?? message.conversationId,
      roomType: message.roomType,
      isRecalled: true,
      updatedAt: message.updatedAt,
    };

    if (message.roomType === 'GROUP') {
      this.transportService.emitToGroup(
        message.roomId ?? message.conversationId,
        CHAT_EVENTS.RECALLED,
        payload,
      );
      return;
    }

    this.transportService.emitToUsers(
      [message.senderId, message.receiverId],
      CHAT_EVENTS.RECALLED,
      payload,
    );
  }

  @SubscribeMessage(CHAT_EVENTS.TYPING)
  async onTyping(
    @ConnectedSocket() client: Socket,
    @MessageBody() payload: TypingDto,
  ): Promise<void> {
    const userId = this.getUserId(client);
    const { receiverId, eventPayload } = await this.chatService.validateTyping(
      userId,
      payload.receiverId,
      payload.isTyping,
    );

    this.transportService.emitToUser(
      receiverId,
      CHAT_EVENTS.TYPING,
      eventPayload,
    );
  }

  @SubscribeMessage(CHAT_EVENTS.RECEIVE)
  async onReceive(
    @ConnectedSocket() client: Socket,
    @MessageBody() payload: ReceiveDto,
  ): Promise<void> {
    const userId = this.getUserId(client);
    const message = await this.chatService.markMessageReceived(
      userId,
      payload.messageId,
    );

    if (!message?.receivedAt) {
      return;
    }

    if (message.roomType === 'GROUP') {
      return;
    }

    this.transportService.emitToUsers(
      [message.senderId, message.receiverId],
      CHAT_EVENTS.RECEIVED,
      {
        messageId: message.messageId,
        senderId: message.senderId,
        receiverId: message.receiverId,
        receivedAt: message.receivedAt,
        updatedAt: message.updatedAt,
      },
    );
  }

  @SubscribeMessage(CHAT_EVENTS.READ)
  async onReadRoom(
    @ConnectedSocket() client: Socket,
    @MessageBody() payload: ReadRoomDto,
  ): Promise<void> {
    const userId = this.getUserId(client);
    await this.chatService.markRoomAsRead(userId, payload.roomId);
  }

  @SubscribeMessage(CHAT_EVENTS.GROUP_READ)
  async onGroupReadRoom(
    @ConnectedSocket() client: Socket,
    @MessageBody() payload: ReadRoomDto,
  ): Promise<void> {
    const userId = this.getUserId(client);
    await this.chatService.markRoomAsRead(userId, payload.roomId);
  }

  @SubscribeMessage(CHAT_EVENTS.GROUP_TYPING)
  async onGroupTyping(
    @ConnectedSocket() client: Socket,
    @MessageBody() payload: { roomId: string; isTyping: boolean },
  ): Promise<void> {
    const userId = this.getUserId(client);
    await this.chatService.ensureGroupMembership(userId, payload.roomId);

    this.transportService.emitToGroup(
      payload.roomId,
      CHAT_EVENTS.GROUP_TYPING,
      {
        roomId: payload.roomId,
        senderId: userId,
        isTyping: payload.isTyping,
      },
    );
  }

  @SubscribeMessage(CHAT_EVENTS.GROUP_CALL_START)
  async onGroupCallStart(
    @ConnectedSocket() client: Socket,
    @MessageBody() payload: GroupCallStartDto,
  ): Promise<void> {
    const userId = this.getUserId(client);
    await this.chatService.ensureGroupMembership(userId, payload.roomId);

    const participants =
      this.activeGroupCallParticipants.get(payload.roomId) ?? new Set<string>();

    const isAlreadyInCall = participants.has(userId);
    if (!isAlreadyInCall && participants.size >= this.maxGroupCallParticipants) {
      this.transportService.emitToUser(
        userId,
        CHAT_EVENTS.GROUP_CALL_LIMIT_REACHED,
        {
          roomId: payload.roomId,
          callMode: payload.callMode,
          maxParticipants: this.maxGroupCallParticipants,
          message: `Cuoc goi nhom tam thoi gioi han toi da ${this.maxGroupCallParticipants} nguoi.`,
        },
      );
      return;
    }

    participants.add(userId);
    this.activeGroupCallParticipants.set(payload.roomId, participants);

    this.transportService.emitToGroup(
      payload.roomId,
      CHAT_EVENTS.GROUP_CALL_STARTED,
      {
        senderId: userId,
        roomId: payload.roomId,
        callMode: payload.callMode,
      },
    );
  }

  @SubscribeMessage(CHAT_EVENTS.GROUP_CALL_OFFER)
  async onGroupCallOffer(
    @ConnectedSocket() client: Socket,
    @MessageBody() payload: GroupCallSignalDto,
  ): Promise<void> {
    const userId = this.getUserId(client);
    await this.chatService.ensureGroupMembership(userId, payload.roomId);
    await this.chatService.ensureGroupMembership(
      payload.receiverId,
      payload.roomId,
    );

    this.transportService.emitToUser(
      payload.receiverId,
      CHAT_EVENTS.GROUP_CALL_OFFER,
      {
        senderId: userId,
        receiverId: payload.receiverId,
        roomId: payload.roomId,
        callMode: payload.callMode,
        offer: payload.offer,
      },
    );
  }

  @SubscribeMessage(CHAT_EVENTS.GROUP_CALL_ANSWER)
  async onGroupCallAnswer(
    @ConnectedSocket() client: Socket,
    @MessageBody() payload: GroupCallSignalDto,
  ): Promise<void> {
    const userId = this.getUserId(client);
    await this.chatService.ensureGroupMembership(userId, payload.roomId);
    await this.chatService.ensureGroupMembership(
      payload.receiverId,
      payload.roomId,
    );

    this.transportService.emitToUser(
      payload.receiverId,
      CHAT_EVENTS.GROUP_CALL_ANSWER,
      {
        senderId: userId,
        receiverId: payload.receiverId,
        roomId: payload.roomId,
        callMode: payload.callMode,
        answer: payload.answer,
      },
    );
  }

  @SubscribeMessage(CHAT_EVENTS.GROUP_CALL_ICE)
  async onGroupCallIce(
    @ConnectedSocket() client: Socket,
    @MessageBody() payload: GroupCallSignalDto,
  ): Promise<void> {
    const userId = this.getUserId(client);
    await this.chatService.ensureGroupMembership(userId, payload.roomId);
    await this.chatService.ensureGroupMembership(
      payload.receiverId,
      payload.roomId,
    );

    this.transportService.emitToUser(
      payload.receiverId,
      CHAT_EVENTS.GROUP_CALL_ICE,
      {
        senderId: userId,
        receiverId: payload.receiverId,
        roomId: payload.roomId,
        callMode: payload.callMode,
        candidate: payload.candidate,
      },
    );
  }

  @SubscribeMessage(CHAT_EVENTS.GROUP_CALL_END)
  async onGroupCallEnd(
    @ConnectedSocket() client: Socket,
    @MessageBody() payload: GroupCallEndDto,
  ): Promise<void> {
    const userId = this.getUserId(client);
    await this.chatService.ensureGroupMembership(userId, payload.roomId);

    const callMode = payload.callMode ?? 'voice';
    const endForAll = payload.endForAll ?? false;

    if (!endForAll) {
      const participants = this.activeGroupCallParticipants.get(payload.roomId);
      participants?.delete(userId);
      if (participants && participants.size === 0) {
        this.activeGroupCallParticipants.delete(payload.roomId);
      }

      const callLogMessage = await this.chatService.createGroupCallLeaveSystemMessage(
        userId,
        payload.roomId,
        { mode: callMode },
      );

      this.transportService.emitToGroup(
        payload.roomId,
        CHAT_EVENTS.GROUP_CALL_END,
        {
          senderId: userId,
          roomId: payload.roomId,
          callMode,
          reason: payload.reason,
          endForAll: false,
          callLogMessage,
        },
      );

      this.transportService.emitToGroup(
        payload.roomId,
        CHAT_EVENTS.GROUP_MESSAGE,
        callLogMessage,
      );
      return;
    }

    const outcome =
      payload.reason === 'cancelled' ? 'self_cancelled' : 'connected_ended';

    this.activeGroupCallParticipants.delete(payload.roomId);

    const callLogMessage = await this.chatService.createGroupCallLogMessage(
      userId,
      payload.roomId,
      { mode: callMode, outcome },
    );

    this.transportService.emitToGroup(
      payload.roomId,
      CHAT_EVENTS.GROUP_CALL_END,
      {
        senderId: userId,
        roomId: payload.roomId,
        callMode,
        reason: payload.reason,
        endForAll: true,
        callLogMessage,
      },
    );

    this.transportService.emitToGroup(
      payload.roomId,
      CHAT_EVENTS.GROUP_MESSAGE,
      callLogMessage,
    );
  }

  @SubscribeMessage(CALL_EVENTS.OFFER)
  async onCallOffer(
    @ConnectedSocket() client: Socket,
    @MessageBody() payload: CallOfferDto,
  ): Promise<void> {
    const userId = this.getUserId(client);
    const { receiverId, eventPayload } =
      await this.chatService.validateCallOffer(userId, payload);

    this.transportService.emitToUser(
      receiverId,
      CALL_EVENTS.OFFER,
      eventPayload,
    );
  }

  @SubscribeMessage(CALL_EVENTS.ANSWER)
  async onCallAnswer(
    @ConnectedSocket() client: Socket,
    @MessageBody() payload: CallAnswerDto,
  ): Promise<void> {
    const userId = this.getUserId(client);
    const { receiverId, eventPayload } =
      await this.chatService.validateCallAnswer(userId, payload);

    this.transportService.emitToUser(
      receiverId,
      CALL_EVENTS.ANSWER,
      eventPayload,
    );
  }

  @SubscribeMessage(CALL_EVENTS.ICE)
  async onCallIce(
    @ConnectedSocket() client: Socket,
    @MessageBody() payload: CallIceDto,
  ): Promise<void> {
    const userId = this.getUserId(client);
    const { receiverId, eventPayload } = await this.chatService.validateCallIce(
      userId,
      payload,
    );

    this.transportService.emitToUser(receiverId, CALL_EVENTS.ICE, eventPayload);
  }

  @SubscribeMessage(CALL_EVENTS.END)
  async onCallEnd(
    @ConnectedSocket() client: Socket,
    @MessageBody() payload: CallEndDto,
  ): Promise<void> {
    const userId = this.getUserId(client);
    const { receiverId, eventPayload } = await this.chatService.validateCallEnd(
      userId,
      payload,
    );

    this.transportService.emitToUser(receiverId, CALL_EVENTS.END, eventPayload);
  }

  broadcastChatMessage(message: {
    senderId: string;
    receiverId: string;
  }): void {
    this.transportService.emitToUsers(
      [message.senderId, message.receiverId],
      CHAT_EVENTS.MESSAGE,
      message,
    );
  }

  emitMessage(message: any): void {
    if (message.roomType === 'GROUP') {
      this.transportService.emitToGroup(
        message.roomId ?? message.conversationId,
        CHAT_EVENTS.GROUP_MESSAGE,
        message,
      );
    } else {
      this.transportService.emitToUsers(
        [message.senderId, message.receiverId],
        CHAT_EVENTS.MESSAGE,
        message,
      );
    }
  }

  private getUserId(client: Socket): string {
    const userId = client.data.userId as string | undefined;
    if (!userId) {
      throw new UnauthorizedException('Unauthorized socket');
    }

    return userId;
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

  broadcastIncomingCall(
    receiverId: string,
    payload: IncomingCallPayload,
  ): void {
    this.transportService.broadcastIncomingCall(receiverId, payload);
  }

  subscribeUserToGroupRoom(userId: string, roomId: string): void {
    this.transportService.subscribeUserToGroupRoom(userId, roomId);
  }

  unsubscribeUserFromGroupRoom(userId: string, roomId: string): void {
    this.transportService.unsubscribeUserFromGroupRoom(userId, roomId);
  }

  broadcastGroupMessage(roomId: string, message: unknown): void {
    this.transportService.broadcastGroupMessage(roomId, message);
  }

  broadcastGroupRoomUpdate(roomId: string, payload: unknown): void {
    this.transportService.broadcastGroupRoomUpdate(roomId, payload);
  }

  broadcastGroupRoomUpdateToUsers(userIds: string[], payload: unknown): void {
    this.transportService.broadcastGroupRoomUpdateToUsers(userIds, payload);
  }

  broadcastGroupMemberLeft(
    roomId: string,
    userId: string,
    payload: unknown,
  ): void {
    this.transportService.broadcastGroupMemberLeft(roomId, userId, payload);
  }

  broadcastGroupDissolved(
    roomId: string,
    memberUserIds: string[],
    payload: unknown,
  ): void {
    this.transportService.broadcastGroupDissolved(
      roomId,
      memberUserIds,
      payload,
    );
  }
}
