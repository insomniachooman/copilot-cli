import type { Context } from "hono"
import consola from "consola"
import { getTokenCount } from "~/lib/tokenizer"
import type { Message } from "~/services/copilot/create-chat-completions"

interface GeminiContent {
  role: string
  parts: Array<{
    text: string
  }>
}

interface GeminiTokenCountRequest {
  contents: GeminiContent[]
}

export async function handleTokenCount(c: Context) {
  try {
    const body = await c.req.json<GeminiTokenCountRequest>()
    consola.debug("Token count request:", JSON.stringify(body))

    // Convert Gemini format to OpenAI format for token counting
    const messages: Message[] = body.contents.map((content) => ({
      role: content.role === "user" ? "user" : "assistant",
      content: content.parts.map((part) => part.text).join(" "),
    }))

    const tokenCounts = getTokenCount(messages)
    const totalTokens = tokenCounts.input + tokenCounts.output

    const response = {
      totalTokens,
    }

    consola.info(`Token count: ${totalTokens}`)
    return c.json(response)
  } catch (error) {
    consola.error("Error counting tokens:", error)
    return c.json({ error: "Failed to count tokens" }, 500)
  }
}
