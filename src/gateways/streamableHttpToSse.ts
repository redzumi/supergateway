import express from 'express'
import bodyParser from 'body-parser'
import cors, { type CorsOptions } from 'cors'
import { Server } from '@modelcontextprotocol/sdk/server/index.js'
import { SSEServerTransport } from '@modelcontextprotocol/sdk/server/sse.js'
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js'
import {
  JSONRPCMessage,
  JSONRPCRequest,
  ClientCapabilities,
  Implementation,
} from '@modelcontextprotocol/sdk/types.js'
import { InitializeRequestSchema } from '@modelcontextprotocol/sdk/types.js'
import { z } from 'zod'
import { getVersion } from '../lib/getVersion.js'
import { Logger } from '../types.js'
import { onSignals } from '../lib/onSignals.js'
import { serializeCorsOrigin } from '../lib/serializeCorsOrigin.js'
import { Client } from '@modelcontextprotocol/sdk/client/index.js'

export interface StreamableHttpToSseArgs {
  streamableHttpUrl: string
  port: number
  baseUrl: string
  ssePath: string
  messagePath: string
  logger: Logger
  corsOrigin: CorsOptions['origin']
  healthEndpoints: string[]
  headers: Record<string, string>
}

const setResponseHeaders = ({
  res,
  headers,
}: {
  res: express.Response
  headers: Record<string, string>
}) =>
  Object.entries(headers).forEach(([key, value]) => {
    res.setHeader(key, value)
  })

const newInitializeMcpClient = ({ message }: { message: JSONRPCRequest }) => {
  const clientInfo = message.params?.clientInfo as Implementation | undefined
  const clientCapabilities = message.params?.capabilities as
    | ClientCapabilities
    | undefined

  return new Client(
    {
      name: clientInfo?.name ?? 'supergateway',
      version: clientInfo?.version ?? getVersion(),
    },
    {
      capabilities: clientCapabilities ?? {},
    },
  )
}

const newFallbackMcpClient = async ({
  mcpTransport,
}: {
  mcpTransport: StreamableHTTPClientTransport
}) => {
  const fallbackMcpClient = new Client(
    {
      name: 'supergateway',
      version: getVersion(),
    },
    {
      capabilities: {},
    },
  )

  await fallbackMcpClient.connect(mcpTransport)
  return fallbackMcpClient
}

export async function streamableHttpToSse(args: StreamableHttpToSseArgs) {
  const {
    streamableHttpUrl,
    port,
    baseUrl,
    ssePath,
    messagePath,
    logger,
    corsOrigin,
    healthEndpoints,
    headers,
  } = args

  logger.info(
    `  - Headers: ${Object(headers).length ? JSON.stringify(headers) : '(none)'}`,
  )
  logger.info(`  - port: ${port}`)
  logger.info(`  - streamableHttp: ${streamableHttpUrl}`)
  if (baseUrl) {
    logger.info(`  - baseUrl: ${baseUrl}`)
  }
  logger.info(`  - ssePath: ${ssePath}`)
  logger.info(`  - messagePath: ${messagePath}`)

  logger.info(
    `  - CORS: ${corsOrigin ? `enabled (${serializeCorsOrigin({ corsOrigin })})` : 'disabled'}`,
  )
  logger.info(
    `  - Health endpoints: ${healthEndpoints.length ? healthEndpoints.join(', ') : '(none)'}`,
  )

  onSignals({ logger })

  const mcpTransport = new StreamableHTTPClientTransport(
    new URL(streamableHttpUrl),
    {
      requestInit: {
        headers,
      },
    },
  )

  mcpTransport.onerror = (err) => {
    logger.error('Streamable HTTP error:', err)
  }

  mcpTransport.onclose = () => {
    logger.error('Streamable HTTP connection closed')
    process.exit(1)
  }

  const server = new Server(
    { name: 'supergateway', version: getVersion() },
    { capabilities: {} },
  )

  const wrapResponse = (req: JSONRPCRequest, payload: object) => ({
    jsonrpc: req.jsonrpc || '2.0',
    id: req.id,
    ...payload,
  })

  const sessions: Record<
    string,
    { transport: SSEServerTransport; response: express.Response }
  > = {}

  const app = express()

  if (corsOrigin) {
    app.use(cors({ origin: corsOrigin }))
  }

  app.use((req, res, next) => {
    if (req.path === messagePath) return next()
    return bodyParser.json()(req, res, next)
  })

  for (const ep of healthEndpoints) {
    app.get(ep, (_req, res) => {
      setResponseHeaders({
        res,
        headers,
      })
      res.send('ok')
    })
  }

  let initializedResponse: object | null = null

  app.get(ssePath, async (req, res) => {
    // TODO: move upp to the top of the file
    let mcpClient: Client | undefined

    logger.info(`New SSE connection from ${req.ip}`)

    setResponseHeaders({
      res,
      headers,
    })

    const sseTransport = new SSEServerTransport(`${baseUrl}${messagePath}`, res)
    await server.connect(sseTransport)

    const sessionId = sseTransport.sessionId
    if (sessionId) {
      sessions[sessionId] = { transport: sseTransport, response: res }
    }

    sseTransport.onmessage = async (message: JSONRPCMessage) => {
      const isRequest = 'method' in message && 'id' in message

      if (isRequest) {
        logger.info(
          `SSE → Streamable HTTP (session ${sessionId}): ${JSON.stringify(message)}`,
        )
        const req = message as JSONRPCRequest
        let result

        try {
          if (!mcpClient) {
            if (message.method === 'initialize') {
              mcpClient = newInitializeMcpClient({
                message,
              })

              const originalRequest = mcpClient.request

              mcpClient.request = async function (
                possibleInitRequestMessage,
                ...restArgs
              ) {
                if (
                  InitializeRequestSchema.safeParse(possibleInitRequestMessage)
                    .success &&
                  message.params?.protocolVersion
                ) {
                  // respect the protocol version from the stdio client's init request
                  possibleInitRequestMessage.params!.protocolVersion =
                    message.params.protocolVersion
                }
                result = await originalRequest.apply(this, [
                  possibleInitRequestMessage,
                  ...restArgs,
                ])
                initializedResponse = result as object
                return result
              }

              await mcpClient.connect(mcpTransport)
              mcpClient.request = originalRequest
            } else {
              logger.info(
                'Streamable HTTP client not initialized, creating fallback client',
              )
              mcpClient = await newFallbackMcpClient({ mcpTransport })
            }

            logger.info('Streamable HTTP connected')
          } else {
            if (message.method === 'initialize') {
              logger.info('Streamable HTTP client already initialized')
              result = initializedResponse
            } else {
              result = await mcpClient.request(req, z.any())
            }
          }
        } catch (err) {
          logger.error('Request error:', err)
          const errorCode =
            err && typeof err === 'object' && 'code' in err
              ? (err as any).code
              : -32000
          let errorMsg =
            err && typeof err === 'object' && 'message' in err
              ? (err as any).message
              : 'Internal error'
          const prefix = `MCP error ${errorCode}:`
          if (errorMsg.startsWith(prefix)) {
            errorMsg = errorMsg.slice(prefix.length).trim()
          }
          const errorResp = wrapResponse(req, {
            error: {
              code: errorCode,
              message: errorMsg,
            },
          })
          process.stdout.write(JSON.stringify(errorResp) + '\n')
          return
        }

        const response = wrapResponse(
          req,
          result && result.hasOwnProperty('error')
            ? { error: { ...(result as any).error } }
            : { result: { ...(result as any) } },
        ) as JSONRPCMessage
        logger.info('Response:', JSON.stringify(response))
        sseTransport.send(response)
      } else {
        logger.info('Streamable HTTP → SSE:', message)
        sseTransport.send(message)
      }

      sseTransport.onclose = () => {
        logger.info(`SSE connection closed (session ${sessionId})`)
        delete sessions[sessionId]
      }

      sseTransport.onerror = (err) => {
        logger.error(`SSE error (session ${sessionId}):`, err)
        delete sessions[sessionId]
      }

      req.on('close', () => {
        logger.info(`Client disconnected (session ${sessionId})`)
        delete sessions[sessionId]
      })
    }
  })

  // @ts-ignore
  app.post(messagePath, async (req, res) => {
    const sessionId = req.query.sessionId as string

    setResponseHeaders({
      res,
      headers,
    })

    if (!sessionId) {
      return res.status(400).send('Missing sessionId parameter')
    }

    const session = sessions[sessionId]
    if (session?.transport?.handlePostMessage) {
      logger.info(`POST to SSE transport (session ${sessionId})`)
      await session.transport.handlePostMessage(req, res)
    } else {
      res.status(503).send(`No active SSE connection for session ${sessionId}`)
    }
  })

  app.listen(port, () => {
    logger.info(`Listening on port ${port}`)
    logger.info(`SSE endpoint: http://localhost:${port}${ssePath}`)
    logger.info(`POST messages: http://localhost:${port}${messagePath}`)
  })
}
