import { createCommandResult } from '../../utils/messages'
import { createLogger } from '../../factories/logger-factory'
import { Event } from '../../@types/event'
import { IEventRepository } from '../../@types/repositories'
import { IEventStrategy } from '../../@types/message-handlers'
import { IWebSocketAdapter } from '../../@types/adapters'
import { WebSocketAdapterEvent } from '../../constants/adapter'

const debug = createLogger('replaceable-event-strategy')
const UNIQUE_VIOLATION_CODE = '23505'
const DUPLICATE_EVENT_ID_CONSTRAINTS = new Set([
  'events_event_id_unique',
  'events_hot_event_id_idx',
])

type PgErrorLike = {
  code?: unknown
  constraint?: unknown
  cause?: unknown
  originalError?: unknown
}

function toPgErrorLike(error: unknown): PgErrorLike | null {
  return (typeof error === 'object' && error !== null) ? (error as PgErrorLike) : null
}

function isDuplicateEventIdError(error: unknown): boolean {
  const pgError = toPgErrorLike(error)
  if (!pgError) {
    return false
  }

  const candidates = [
    pgError,
    toPgErrorLike(pgError.cause),
    toPgErrorLike(pgError.originalError),
  ].filter((candidate): candidate is PgErrorLike => candidate !== null)

  return candidates.some((candidate) =>
    candidate.code === UNIQUE_VIOLATION_CODE
    && typeof candidate.constraint === 'string'
    && DUPLICATE_EVENT_ID_CONSTRAINTS.has(candidate.constraint)
  )
}

export class ReplaceableEventStrategy implements IEventStrategy<Event, Promise<void>> {
  public constructor(
    private readonly webSocket: IWebSocketAdapter,
    private readonly eventRepository: IEventRepository,
  ) { }

  public async execute(event: Event): Promise<void> {
    debug('received replaceable event: %o', event)
    try {
      const count = await this.eventRepository.upsert(event)
      this.webSocket.emit(
        WebSocketAdapterEvent.Message,
        createCommandResult(event.id, true, (count) ? '' : 'duplicate:'),
      )
      if (count) {
        this.webSocket.emit(WebSocketAdapterEvent.Broadcast, event)
      }
    } catch (error: unknown) {
      if (isDuplicateEventIdError(error)) {
        this.webSocket.emit(
          WebSocketAdapterEvent.Message,
          createCommandResult(event.id, false, 'rejected: event already exists'),
        )
        return
      }

      this.webSocket.emit(
        WebSocketAdapterEvent.Message,
        createCommandResult(event.id, false, 'error: '),
      )
    }
  }
}
