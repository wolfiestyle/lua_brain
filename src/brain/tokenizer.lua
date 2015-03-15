-- Utility functions for processing text.
-- Author: darkstalker <https://github.com/darkstalker>
-- License: MIT/X11
local table_concat =
      table.concat
local utf8 = require "utf8"

local tokenizer = {}

-- Splits the input text into tokens
function tokenizer.parse(text)
    local tokens = {}
    local n = 1
    for fragment in utf8.gmatch(text, "%S+") do
        -- url
        if fragment:find "^%a[%w%-+.]*://" then
            tokens[n] = { type = "u", pos = 0, text = fragment }
            n = n + 1
        else
            local prefix, word, suffix = utf8.match(fragment, "^(%p*)(.-)(%p*)$")
            local token
            -- hashtag / mention
            local p, c = prefix:match "(.-)([#@])$"
            if p then
                token = { type = c, pos = 0, text = c .. word }
                prefix = p
            else
                -- lowercase or capitalized word
                if utf8.find(word, "^%u?[%l%d_']+$") then
                    token = { type = "w", pos = 0, text = utf8.lower(word) }
                -- uppercase/mixed case word
                elseif utf8.find(word, "^[%u%d_']+$") or utf8.find(word, "^[%w_']+$") then
                    token = { type = "w", pos = 0, text = word }
                -- unknown
                elseif word ~= "" then
                    token = { type = "_", pos = 0, text = word }
                end
            end
            if prefix ~= "" then
                tokens[n] = { type = "p", pos = token and 2 or 0, text = prefix }
                n = n + 1
            end
            if token then
                tokens[n] = token
                n = n + 1
            end
            if suffix ~= "" then
                tokens[n] = { type = "p", pos = 1, text = suffix }
                n = n + 1
            end
        end
    end
    return tokens
end

-- Creates a string by joining tokens
function tokenizer.compose(tokens)
    local text = {}
    local n = #tokens
    for i = 1, n do
        local item = tokens[i]
        text[#text + 1] = item.text
        if item.pos ~= 2 and i < n and tokens[i + 1].pos ~= 1 then
            text[#text + 1] = " "
        end
    end
    return table_concat(text)
end

return tokenizer
