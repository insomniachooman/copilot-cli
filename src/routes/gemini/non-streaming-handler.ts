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

interface GeminiRequest {
  contents: GeminiContent[]
  generationConfig?: {
    temperature?: number
    maxOutputTokens?: number
    topP?: number
    topK?: number
  }
}

// Google genai API response format (exact format expected by Gemini CLI)
interface GoogleGenaiResponse {
  candidates: Array<{
    content: {
      role: string
      parts: Array<{
        text: string
      }>
    }
    finishReason: string
    safetyRatings?: Array<any>
  }>
  usageMetadata: {
    promptTokenCount: number
    candidatesTokenCount: number
    totalTokenCount: number
  }
}

export async function handleGeminiNonStreaming(c: Context) {
  await checkRateLimit(state)

  try {
    const body = await c.req.json<GeminiRequest>()
    consola.debug("Gemini non-streaming request:", JSON.stringify(body))

    // Convert Gemini format to OpenAI format
    const messages: Message[] = body.contents.map((content) => ({
      role: content.role === "model" ? "assistant" : "user",
      content: content.parts.map((part) => part.text).join(" "),
    })) as Message[]

    // If geminiCliModel is set, use it
    const model = state.geminiCliModel || "gpt-4"

    // Create the payload for OpenAI-style completion
    const payload: ChatCompletionsPayload = {
      model,
      messages,
      stream: false,
      temperature: body.generationConfig?.temperature,
      max_tokens: body.generationConfig?.maxOutputTokens,
      top_p: body.generationConfig?.topP,
    }

    consola.info("Current token count:", getTokenCount(messages))

    if (state.manualApprove) await awaitApproval()

    const response = await createChatCompletions(payload)

    // Transform OpenAI response to Gemini format
    if ('choices' in response && response.choices.length > 0) {
      const choice = response.choices[0]
      const responseText = choice.message?.content || ""
      
      const tokenCounts = getTokenCount([...messages, { role: "assistant", content: responseText }])

      const geminiResponse: GoogleGenaiResponse = {
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
            finishReason: choice.finish_reason === "stop" ? "STOP" : "OTHER",
          },
        ],
        usageMetadata: {
          promptTokenCount: tokenCounts.input,
          candidatesTokenCount: tokenCounts.output,
          totalTokenCount: tokenCounts.input + tokenCounts.output,
        },
      }

      return c.json(geminiResponse)
    } else {
      throw new Error("Invalid response from chat completions")
    }
  } catch (error) {
    consola.error("Error in Gemini non-streaming:", error)
    
    // Return proper Google genai format error response
    const errorResponse: GoogleGenaiResponse = {
      candidates: [
        {
          content: {
            role: "model",
            parts: [
              {
                text: "",
              },
            ],
          },
          finishReason: "OTHER",
        },
      ],
      usageMetadata: {
        promptTokenCount: 0,
        candidatesTokenCount: 0,
        totalTokenCount: 0,
      },
    }
    
    return c.json(errorResponse, 500)
  }
}
