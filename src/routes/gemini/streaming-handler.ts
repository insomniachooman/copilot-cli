import type { Context } from "hono"
import consola from "consola"

import { awaitApproval } from "~/lib/approval"
import { checkRateLimit } from "~/lib/rate-limit"
import { state } from "~/lib/state"
import { getTokenCount } from "~/lib/tokenizer"
import {
  createChatCompletions,
  type ChatCompletionsPayload,
  type Message,
} from "~/services/copilot/create-chat-completions"

interface GeminiContent {
  role: string
  parts: Array<{
    text: string
  }>
}

interface GeminiStreamRequest {
  contents: GeminiContent[]
  generationConfig?: {
    temperature?: number
    maxOutputTokens?: number
    topP?: number
    topK?: number
  }
}

// Google genai API streaming response format (exact format expected by Gemini CLI)
interface GoogleGenaiStreamResponse {
  candidates: Array<{
    content?: {
      role: string
      parts: Array<{
        text?: string
      }>
    }
    finishReason?: string
    safetyRatings?: Array<any>
  }>
  usageMetadata?: {
    promptTokenCount: number
    candidatesTokenCount: number
    totalTokenCount: number
  }
}

export async function handleGeminiStreaming(c: Context) {
  consola.info("=== GEMINI STREAMING HANDLER CALLED ===")
  await checkRateLimit(state)

  try {
    const body = await c.req.json<GeminiStreamRequest>()
    consola.info("Gemini streaming request received:", JSON.stringify(body, null, 2))

    // Convert Gemini format to OpenAI format
    const messages: Message[] = body.contents.map((content) => ({
      role: content.role === "model" ? "assistant" : "user",
      content: content.parts.map((part) => part.text).join(" "),
    })) as Message[]

    // If geminiCliModel is set, use it, otherwise use a reliable streaming model
    const model = state.geminiCliModel || "gpt-4o"

    // Create the payload for OpenAI-style completion
    const payload: ChatCompletionsPayload = {
      model,
      messages,
      stream: true,
      temperature: body.generationConfig?.temperature,
      max_tokens: body.generationConfig?.maxOutputTokens,
      top_p: body.generationConfig?.topP,
    }

    consola.info("Current token count:", getTokenCount(messages))
    consola.info("About to call createChatCompletions with payload:", JSON.stringify(payload, null, 2))

    if (state.manualApprove) await awaitApproval()

    const response = await createChatCompletions(payload)
    consola.info("createChatCompletions returned:", typeof response)
    consola.info("Response details:", {
      isAsyncIterable: Symbol.asyncIterator in response,
      hasChoices: 'choices' in response,
      constructor: response.constructor.name
    })

    consola.info("Response type:", typeof response, "has choices:", 'choices' in response)

    // Handle non-streaming response by converting to streaming format
    if ('choices' in response) {
      consola.warn("Received non-streaming response for streaming request, converting to streaming format")
      
      const choice = response.choices[0]
      const responseText = choice.message?.content || ""
      
      consola.info("Non-streaming response content:", responseText)
      
      const tokenCounts = getTokenCount([...messages, { role: "assistant", content: responseText }])

      // Create raw SSE response for Gemini CLI compatibility
      c.header('Content-Type', 'text/event-stream; charset=utf-8')
      c.header('Cache-Control', 'no-cache')
      c.header('Connection', 'keep-alive')
      c.header('Access-Control-Allow-Origin', '*')
      c.header('Access-Control-Allow-Headers', 'Content-Type')
      c.header('X-Accel-Buffering', 'no')

      const stream = new ReadableStream({
        start(controller) {
          const encoder = new TextEncoder()
          
          try {
            // Send the content chunk in exact Google Gemini API format
            const contentChunk: GoogleGenaiStreamResponse = {
              candidates: [
                {
                  content: {
                    role: "model",
                    parts: [
                      {
                        text: responseText,
                      },
                    ],
                  },
                },
              ],
            }

            const contentData = `data: ${JSON.stringify(contentChunk)}\n\n`
            consola.debug("Sending content chunk:", contentData)
            controller.enqueue(encoder.encode(contentData))

            // Send the final chunk with usage metadata
            const finalChunk: GoogleGenaiStreamResponse = {
              candidates: [
                {
                  content: {
                    role: "model",
                    parts: [{ text: "" }],
                  },
                  finishReason: choice.finish_reason === "stop" ? "STOP" : "OTHER",
                },
              ],
              usageMetadata: {
                promptTokenCount: tokenCounts.input,
                candidatesTokenCount: tokenCounts.output,
                totalTokenCount: tokenCounts.input + tokenCounts.output,
              },
            }

            const finalData = `data: ${JSON.stringify(finalChunk)}\n\n`
            consola.debug("Sending final chunk:", finalData)
            controller.enqueue(encoder.encode(finalData))
            
            // End the stream properly for Gemini CLI
            const doneData = 'data: [DONE]\n\n'
            consola.debug("Sending DONE signal:", doneData)
            controller.enqueue(encoder.encode(doneData))
            
            controller.close()
            consola.debug("Stream closed successfully")
          } catch (error) {
            consola.error("Error in non-streaming SSE conversion:", error)
            controller.error(error)
          }
        }
      })

      return c.body(stream)
    }

    // Stream the response in raw SSE format for Gemini CLI
    c.header('Content-Type', 'text/event-stream; charset=utf-8')
    c.header('Cache-Control', 'no-cache')
    c.header('Connection', 'keep-alive')
    c.header('Access-Control-Allow-Origin', '*')
    c.header('Access-Control-Allow-Headers', 'Content-Type')
    c.header('X-Accel-Buffering', 'no')

    const stream = new ReadableStream({
      start(controller) {
        const encoder = new TextEncoder()
        let accumulatedText = ""

        const processStream = async () => {
          try {
            consola.info("Starting to process stream")
            
            for await (const event of response) {
              consola.info("Received streaming event:", event)
              
              // Handle SSE message with data field
              if (event.data) {
                const eventData = event.data
                
                // Skip empty data or heartbeat events
                if (!eventData || eventData.trim() === '') {
                  continue
                }
                
                // Check for stream end
                if (eventData === "[DONE]") {
                  consola.debug("Stream ended with [DONE]")
                  break
                }

                let chunk
                try {
                  chunk = JSON.parse(eventData)
                } catch (parseError) {
                  consola.warn("Failed to parse SSE chunk:", eventData, parseError)
                  continue
                }

                // Handle content chunks from OpenAI format
                if (chunk.choices?.[0]?.delta?.content) {
                  accumulatedText += chunk.choices[0].delta.content

                  const geminiChunk: GoogleGenaiStreamResponse = {
                    candidates: [
                      {
                        content: {
                          role: "model",
                          parts: [
                            {
                              text: chunk.choices[0].delta.content,
                            },
                          ],
                        },
                      },
                    ],
                  }

                  const chunkData = `data: ${JSON.stringify(geminiChunk)}\n\n`
                  consola.debug("Sending chunk:", chunkData)
                  controller.enqueue(encoder.encode(chunkData))
                }

                // Handle the finish reason
                if (chunk.choices?.[0]?.finish_reason) {
                  consola.debug("Stream finished with reason:", chunk.choices[0].finish_reason)
                  
                  const tokenCounts = getTokenCount([
                    ...messages,
                    { role: "assistant", content: accumulatedText },
                  ])

                  const finalChunk: GoogleGenaiStreamResponse = {
                    candidates: [
                      {
                        content: {
                          role: "model",
                          parts: [{ text: "" }],
                        },
                        finishReason:
                          chunk.choices[0].finish_reason === "stop" ? "STOP" : "OTHER",
                      },
                    ],
                    usageMetadata: {
                      promptTokenCount: tokenCounts.input,
                      candidatesTokenCount: tokenCounts.output,
                      totalTokenCount: tokenCounts.input + tokenCounts.output,
                    },
                  }

                  const finalData = `data: ${JSON.stringify(finalChunk)}\n\n`
                  consola.debug("Sending final chunk:", finalData)
                  controller.enqueue(encoder.encode(finalData))
                  
                  const doneData = 'data: [DONE]\n\n'
                  consola.debug("Sending DONE signal:", doneData)
                  controller.enqueue(encoder.encode(doneData))
                  break
                }
              }
            }
          } catch (streamError) {
            consola.error("Error processing stream:", streamError)
            // Send an error chunk to properly close the stream
            try {
              const errorChunk: GoogleGenaiStreamResponse = {
                candidates: [{
                  content: { role: "model", parts: [{ text: "" }] },
                  finishReason: "OTHER"
                }],
                usageMetadata: {
                  promptTokenCount: 0,
                  candidatesTokenCount: 0,
                  totalTokenCount: 0,
                },
              }

              const errorData = `data: ${JSON.stringify(errorChunk)}\n\n`
              controller.enqueue(encoder.encode(errorData))
              const doneData = 'data: [DONE]\n\n'
              controller.enqueue(encoder.encode(doneData))
            } catch (writeError) {
              consola.error("Failed to write error chunk:", writeError)
            }
          } finally {
            consola.debug("Stream processing finished, closing controller")
            controller.close()
          }
        }

        processStream()
      }
    })

    return c.body(stream)
  } catch (error) {
    consola.error("Error in Gemini streaming:", error)
    return c.json({ error: "Failed to process streaming request" }, 500)
  }
}
