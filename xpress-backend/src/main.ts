import 'dotenv/config';
import { ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { IoAdapter } from '@nestjs/platform-socket.io';
import { ServerOptions } from 'socket.io';
import { AppModule } from './app.module';

/**
 * Browser origins allowed by REST and Socket.IO.
 * Supports comma-separated exact origins and regex literals, e.g.
 * CORS_ORIGINS=http://localhost:3000,/^http:\/\/192\.168\.\d+\.\d+(:\d+)?$/
 */
function corsOriginsFromEnv(): (string | RegExp)[] {
  const raw = (process.env.CORS_ORIGINS ?? '').trim();
  if (!raw) return [];

  return raw
    .split(',')
    .map((s) => s.trim())
    .filter((s) => s.length > 0)
    .map((origin) => {
      const regexMatch = origin.match(/^\/(.+)\/([a-z]*)$/i);
      if (!regexMatch) return origin;

      const [, pattern, flags] = regexMatch;
      return new RegExp(pattern, flags);
    });
}

const corsOriginList = corsOriginsFromEnv();

class SocketIoCorsAdapter extends IoAdapter {
  override createIOServer(port: number, options?: ServerOptions) {
    // eslint-disable-next-line @typescript-eslint/no-unsafe-return
    return super.createIOServer(port, {
      ...options,
      cors: {
        ...(options?.cors ?? {}),
        origin: corsOriginList,
        methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
        credentials: true,
      },
    });
  }
}

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  app.enableCors({
    origin: corsOriginList,
    methods: 'GET,HEAD,PUT,PATCH,POST,DELETE,OPTIONS',
    credentials: true,
    allowedHeaders: 'Content-Type, Authorization, Idempotency-Key',
  });

  // Configure Socket.IO adapter with CORS
  app.useWebSocketAdapter(new SocketIoCorsAdapter(app));

  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      transform: true,
    }),
  );
  await app.listen(process.env.PORT ?? 3000);
}
void bootstrap();
