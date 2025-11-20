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
 * Decoder that transforms JSONValue into user's V type.
 *
 * Users provide this to parse JSON from S3 into their custom types.
 *
 * @param jsonValue - The JSON value to decode
 * @returns The decoded value of type V
 * @throws Error if the JSON structure is invalid for type V
 */
export type ValueDecoder<V> = (jsonValue: JSONValue) => V;

/**
 * Encoder that transforms user's V type into JSONValue.
 *
 * Users provide this to serialize their custom types to JSON for S3.
 *
 * @param value - The value to encode
 * @returns The encoded JSON value
 */
export type ValueEncoder<V> = (value: V) => JSONValue;
