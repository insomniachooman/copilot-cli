import { Hono } from "hono"
import { handleTokenCount } from "./handler"

export const tokenCountRoutes = new Hono()

// Handle the Gemini-style token counting endpoint
// The :countTokens part is treated as a literal string in the route pattern
tokenCountRoutes.post("/:model\\:countTokens", handleTokenCount)
