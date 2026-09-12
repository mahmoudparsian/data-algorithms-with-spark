-- Pandoc Lua filter used when building data_design_patterns.html/.pdf.
--
-- Reproduces GitHub's heading-slug algorithm (lowercase, drop punctuation,
-- spaces -> hyphens, keep leading digits) so the chapter's own hand-written
-- table of contents links (e.g. "#7-inputoutput-patterns") resolve
-- correctly. Pandoc's built-in auto-identifiers strip a leading digit,
-- which would otherwise break every anchor in the TOC.
local seen = {}

local function slugify(text)
  text = text:lower()
  text = text:gsub("[^%w%s%-]", "")
  text = text:gsub("%s+", "-")
  return text
end

function Header(el)
  local text = pandoc.utils.stringify(el.content)
  local id = slugify(text)
  if seen[id] then
    seen[id] = seen[id] + 1
    id = id .. "-" .. seen[id]
  else
    seen[id] = 0
  end
  el.identifier = id
  return el
end
