/**
 * Public JSON type definitions for ImmuKV.
 *
 * Only exports types that users need for custom value parsing.
 * Internal helper functions are in ./internal/jsonHelpers.
 */

/**
 * Represents any valid JSON value.
 */
export type JSONValue =
  | null
  | boolean
  | number
  | string
  | JSONValue[]
  | { [key: string]: JSONValue };

/**
 * Parser that transforms JSONValue into user's V type.
 *
 * @param jsonValue - The JSON value to parse
 * @returns The parsed value of type V
 * @throws Error if the JSON structure is invalid for type V
 */
export type ValueParser<V> = (jsonValue: JSONValue) => V;
