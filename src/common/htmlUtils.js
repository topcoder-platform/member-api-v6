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
    .replace(/&amp;/gi, '&')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")

  return out
}

module.exports = {
  htmlToText
}
