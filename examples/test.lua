#!/usr/bin/env lua
package.path = "../src/?.lua;" .. package.path
local brain = require "brain"

-- get input from command line
local input = select("#", ...) > 0 and table.concat({ ... }, " ") or nil

-- create the bot instance (SQLite in-memory database, order 1)
local bot = brain.new(":memory:", 1)

-- learn some text
-- begin/end batch increases learning speed (single database transaction)
bot:begin_batch()
for line in io.lines "sample.txt" do
    bot:learn(line)
end
bot:end_batch()

-- print database stats
local stats = bot.db:get_stats()
print(string.format("-- generated %d tokens, %d states, %d transitions", stats.tokens, stats.states, stats.transitions))

-- generate a random reply (up to 140 characters)
print "-- reply:"
math.randomseed(os.time())
print(bot:reply(input, 140))
