const crypto = require('crypto')

const DEFAULT_IDENTIFIER_ALPHABET = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'

/**
 * Generate a cryptographically secure random string with uniformly selected characters.
 * Member deletion uses this helper for anonymized handles and email addresses.
 * @param {number} size number of characters to generate
 * @param {string} alphabet unique characters allowed in the generated value
 * @returns {string} a random string of the requested length
 * @throws {RangeError} when size is not a positive safe integer or alphabet has fewer than two unique characters
 * @throws {TypeError} when alphabet is not a string
 */
function generateSecureRandomString (size = 21, alphabet = DEFAULT_IDENTIFIER_ALPHABET) {
  if (!Number.isSafeInteger(size) || size <= 0) {
    throw new RangeError('size must be a positive safe integer')
  }
  if (typeof alphabet !== 'string') {
    throw new TypeError('alphabet must be a string')
  }
  if (alphabet.length < 2 || new Set(alphabet).size !== alphabet.length) {
    throw new RangeError('alphabet must contain at least two unique characters')
  }

  let value = ''
  for (let index = 0; index < size; index += 1) {
    value += alphabet[crypto.randomInt(alphabet.length)]
  }
  return value
}

module.exports = {
  generateSecureRandomString
}
