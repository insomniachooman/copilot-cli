import { Hono } from "hono"
import consola from "consola"
import { handleTokenCount } from "../token-count/handler"
import { handleGeminiStreaming } from "./streaming-handler"
import { handleGeminiNonStreaming } from "./non-streaming-handler"

export const geminiRoutes = new Hono()

// Order matters! Most specific routes first
// Streaming generation endpoint (most specific)
geminiRoutes.post("/:model\\:streamGenerateContent", (c) => {
  consola.info("=== STREAMING ENDPOINT HIT ===", c.req.path)
  return handleGeminiStreaming(c)
})

// Non-streaming generation endpoint (medium specific)
geminiRoutes.post("/:model\\:generateContent", (c) => {
  consola.info("=== NON-STREAMING ENDPOINT HIT ===", c.req.path)
  return handleGeminiNonStreaming(c)
})

// Token counting endpoint (least specific - should be last)
geminiRoutes.post("/:model\\:countTokens", (c) => {
  consola.info("=== TOKEN COUNT ENDPOINT HIT ===", c.req.path)
  return handleTokenCount(c)
})
