const BASIC_HTML_ENTITY_VALUES = {
  '&quot;': '"',
  '&#39;': "'",
  '&amp;': '&',
  '&lt;': '<',
  '&gt;': '>',
  '&nbsp;': ' '
}

/**
 * Decode one layer of the basic named and numeric HTML entities used by legacy
 * challenge metadata. A single replacement pass preserves nested entity text,
 * preventing an encoded value from being decoded repeatedly across trust boundaries.
 * @param {*} value value read from legacy HTML
 * @returns {string|null} trimmed text with one entity layer decoded, or null for a nullish input
 */
function decodeBasicHtmlEntitiesOnce (value) {
  if (value === null || value === undefined) {
    return null
  }

  return String(value)
    .replace(/&(?:quot|#39|amp|lt|gt|nbsp);/g, entity => BASIC_HTML_ENTITY_VALUES[entity])
    .trim()
}

/**
 * Convert HTML to plain text (strip tags, decode entities).
 * Used for achievements/badge content in PDF so unregistered fonts (e.g. Roboto) are never passed to the renderer.
 * @param {string} html - HTML string
 * @returns {string} plain text
 */
function htmlToText (html) {
  if (!html || typeof html !== 'string') return ''

  let out = html
    // Strip all tags: replace with space so words don't glue together
    .replace(/<[^>]*>/g, ' ')
    // Collapse whitespace
    .replace(/\s+/g, ' ')
    .trim()

  // Decode common HTML entities
  out = out
    .replace(/&nbsp;/gi, ' ')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&amp;/gi, '&')

  return out
}

module.exports = {
  decodeBasicHtmlEntitiesOnce,
  htmlToText
}
