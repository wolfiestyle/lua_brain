-- Algorithms for working with Markov chains.
-- Author: darkstalker <https://github.com/darkstalker>
-- License: MIT/X11
local ipairs, math_random, next, table_insert, table_remove, table_sort =
      ipairs, math.random, next, table.insert, table.remove, table.sort
local unpack = table.unpack or unpack

local engine = {}

-- Learns from the input tokens
function engine.learn(input, order, db)
    local state = {}
    local prev_sid, prev_tid
    for i, tok in ipairs(input) do
        -- add token
        local tid = db:find_token_id(tok.pos, tok.text)
        if tid then
            db:inc_token(tid)
        else
            tid = db:new_token(tok)
        end
        state[#state + 1] = tid
        if i >= order then
            -- add state
            local sid = db:find_state_id(unpack(state))
            if not sid then
                sid = db:new_state(unpack(state))
            end
            if prev_sid then
                -- forward chain
                local trid = db:find_transition_next(prev_sid, tid)
                if trid then
                    db:inc_transition_next(trid)
                else
                    db:new_transition_next(prev_sid, tid)
                end
                -- backward chain
                trid = db:find_transition_prev(sid, prev_tid)
                if trid then
                    db:inc_transition_prev(trid)
                else
                    db:new_transition_prev(sid, prev_tid)
                end
            end
            prev_sid = sid
            prev_tid = table_remove(state, 1)
        end
    end
end

-- Walks randomly over the next_token chain from the specified state
function engine.reply_next(sid, max_iter, order, db)
    local state = db:get_state(sid)
    local result = db:get_token_list(unpack(state))
    for _ = 1, max_iter do
        local next_tid = db:random_transition_next(sid)
        if next_tid == nil then break end
        result[#result + 1] = db:get_token(next_tid)
        table_remove(state, 1)
        state[order] = next_tid
        sid = db:find_state_id(unpack(state))
        if sid == nil then break end
    end
    return result
end

-- Walks randomly over the prev_token chain from the specified state
function engine.reply_prev(sid, max_iter, order, db)
    local state = db:get_state(sid)
    local result = {}
    for _ = 1, max_iter do
        local prev_tid = db:random_transition_prev(sid)
        if prev_tid == nil then break end
        table_insert(result, 1, db:get_token(prev_tid))
        state[order] = nil
        table_insert(state, 1, prev_tid)
        sid = db:find_state_id(unpack(state))
        if sid == nil then break end
    end
    return result
end

-- Chooses a random state based on the input tokens
function engine.choose_pivot(input, db)
    local tokens = {}
    -- get learned tokens
    if input then
        for _, tok in ipairs(input) do
            local db_tok = db:find_token(tok.pos, tok.text)
            if db_tok then
                tokens[#tokens + 1] = db_tok
            end
        end
    end
    if next(tokens) then
        -- sort by count (rarest first)
        table_sort(tokens, function(a, b)
            return a.count < b.count
        end)
        -- pick a random state using the input tokens
        for _, tok in ipairs(tokens) do
            if tok.count > 1 then
                local pivot = db:random_state_with(tok.id)
                if pivot then
                    return pivot
                end
            end
        end
    end
    -- couldn't get a state from the input, pick anything
    return math_random(1, db:state_max_id())
end

return engine
