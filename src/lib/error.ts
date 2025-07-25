import type { Context } from "hono"

import consola from "consola"

export class HTTPError extends Error {
  response: Response

  constructor(message: string, response: Response) {
    super(message)
    this.response = response
  }
}

export async function forwardError(c: Context, error: unknown) {
  consola.error("Error occurred:", error)

  if (error instanceof HTTPError) {
    // Clone the response to safely read the body for logging without consuming it.
    const responseClone = error.response.clone()
    try {
      const errorJson = await responseClone.json()
      consola.error("HTTP error:", errorJson)
    } catch {
      // If parsing as JSON fails, try to read as text using another clone
      const textClone = error.response.clone()
      try {
        const errorText = await textClone.text()
        consola.error("HTTP error (non-JSON):", errorText)
      } catch (textError) {
        consola.error("Failed to read error response body:", textError)
      }
    }

    // Create a new response with the same properties as the original error response
    // This prevents issues with the body being consumed
    const headers = new Headers()
    error.response.headers.forEach((value, key) => {
      headers.set(key, value)
    })

    // Read the body from another clone for the response
    let responseBody: string
    try {
      const bodyClone = error.response.clone()
      responseBody = await bodyClone.text()
    } catch {
      responseBody =
        '{"error": {"message": "Failed to read error response", "code": "unknown_error"}}'
    }

    return new Response(responseBody, {
      status: error.response.status,
      statusText: error.response.statusText,
      headers: headers,
    })
  }

  return c.json(
    {
      error: {
        message: (error as Error).message,
        type: "error",
      },
    },
    500,
  )
}
