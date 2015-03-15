-- Chat bot engine based on Markov chains.
-- Author: darkstalker <https://github.com/darkstalker>
-- License: MIT/X11
local next, select, setmetatable, table_remove =
      next, select, setmetatable, table.remove
local tokenizer = require "brain.tokenizer"
local database = require "brain.database"
local engine = require "brain.engine"

local brain = {}
brain.__index = brain

function brain.new(db_file, order)
    local self = {
        db = database.open(db_file),
        transaction = 0,
    }
    self.order = self.db:init(order)

    return setmetatable(self, brain)
end

function brain:set_filter(filter)
    local opts = {}
    for name in filter:gmatch "[wup#@$_]" do
        opts[name] = true
    end
    self.filter = next(opts) and opts
end

local function filter_list(list, filter)
    for i = #list, 1, -1 do
        local item = list[i]
        if filter[item.type] then
            table_remove(list, i)
        end
    end
end

function brain:learn(...)
    self:begin_batch()
    for i = 1, select("#", ...) do
        local text = select(i, ...)
        local tokens = tokenizer.parse(text)
        if self.filter then
            filter_list(tokens, self.filter)
        end
        engine.learn(tokens, self.order, self.db)
    end
    self:end_batch()
end

local function shorten(str, max_len)
    if #str > max_len then
        local out = str:sub(1, max_len)
        local n = max_len + 1
        if str:sub(n, n) ~= " " then
            out = out:match "(.-) %S*$"
        end
        return out
    end
    return str
end

function brain:reply(text, max_len, max_iter)
    max_iter = max_iter or 20
    local tokens = text and tokenizer.parse(text)
    local pivot = engine.choose_pivot(tokens, self.db)
    local left = engine.reply_prev(pivot, max_iter, self.order, self.db)
    local right = engine.reply_next(pivot, max_iter, self.order, self.db)
    local n = #left
    local sep = n > 0 and left[n].pos ~= 2 and right[1].pos ~= 1 and " " or ""
    local result = tokenizer.compose(left) .. sep ..  tokenizer.compose(right)
    if max_len then
        return shorten(result, max_len)
    end
    return result
end

function brain:begin_batch()
    if self.transaction == 0 then
        self.db:begin_transaction()
    end
    self.transaction = self.transaction + 1
end

function brain:end_batch()
    self.transaction = self.transaction - 1
    if self.transaction == 0 then
        self.db:commit()
    end
end

return brain
