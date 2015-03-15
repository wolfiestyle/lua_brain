-- SQLite database interface for storing a Markov chain.
-- Author: darkstalker <https://github.com/darkstalker>
-- License: MIT/X11
local assert, error, math_random, setmetatable, table_concat, tonumber, tostring =
      assert, error, math.random, setmetatable, table.concat, tonumber, tostring
local sqlite = require "lsqlite3"

local database = {}
database.__index = database

local sql_db_template = [[
CREATE TABLE markov_config (
    key TEXT NOT NULL PRIMARY KEY,
    val TEXT
);
CREATE TABLE token (
    id INTEGER PRIMARY KEY,
    pos INTEGER NOT NULL,
    text TEXT NOT NULL,
    count INTEGER NOT NULL DEFAULT 1,
    UNIQUE (pos, text)
);
CREATE TABLE state (
    id INTEGER PRIMARY KEY,
    %s,
    UNIQUE (%s)
);
CREATE TABLE next_token (
    id INTEGER PRIMARY KEY,
    state_id INTEGER NOT NULL REFERENCES state(id),
    token_id INTEGER NOT NULL REFERENCES token(id),
    count INTEGER NOT NULL DEFAULT 1,
    UNIQUE (state_id, token_id)
);
CREATE TABLE prev_token (
    id INTEGER PRIMARY KEY,
    state_id INTEGER NOT NULL REFERENCES state(id),
    token_id INTEGER NOT NULL REFERENCES token(id),
    count INTEGER NOT NULL DEFAULT 1,
    UNIQUE (state_id, token_id)
);
%s
]]

function database.open(filename)
    local self = {}

    local db, _, err = sqlite.open(filename)
    if db == nil then
        return nil, err
    end
    self.db = db

    -- statement cache
    self.prepare = setmetatable({}, {
        __index = function(_self, sql)
            local st = db:prepare(sql)
            if st == nil then
                error(db:error_message(), 2)
            end
            _self[sql] = st
            return st
        end,
    })

    return setmetatable(self, database)
end

function database:close()
    return self.db:close()
end

function database:init(order)
    if self:table_exists "markov_config" then
        order = assert(tonumber(self:get_config("order")), "invalid config")
        self:_gen_sql(order)
    else
        order = order or 2
        self:create_schema(order)
    end

    -- Weighted Random Sampling [Efraimidis 2005]
    self.db:create_function("random_weighted", 1, function(ctx, weight)
        ctx:result_number(math_random() ^ (1 / weight))
    end)

    self.db:exec "PRAGMA foreign_keys = ON"

    return order
end

function database:_gen_sql(order, with_schema)
    local names, s_cond, s_cond2, args, case = {}, {}, {}, {}, {}
    for i = 1, order do
        local name = ("token%d_id"):format(i)
        names[i] = name
        s_cond[i] = name .. "=?"
        s_cond2[i] = name .. "=$1"
        args[i] = "$" .. i
        case[i] = ("WHEN $%d THEN %d "):format(i, i)
    end

    local names_list = table_concat(names, ",")
    local args_list = "(" .. ("?,"):rep(order):sub(1, -2) .. ")"

    self.sql_get_token_list = ("SELECT * FROM token WHERE id IN (%s) ORDER BY CASE id %sEND"):format(table_concat(args, ","), table_concat(case))
    self.sql_get_state = "SELECT " .. names_list .. " FROM state WHERE id = ?"
    self.sql_find_state_id = "SELECT id FROM state WHERE " .. table_concat(s_cond, " AND ")
    self.sql_new_state = "INSERT INTO state (" .. names_list .. ") VALUES " .. args_list
    self.sql_random_state_with = "SELECT id FROM state WHERE " .. table_concat(s_cond2, " OR ") .. " ORDER BY random() LIMIT 1"

    if with_schema then
        local s_rows, s_idx = {}, {}
        for i = 1, order do
            s_rows[i] = ("token%d_id INTEGER NOT NULL REFERENCES token(id)"):format(i)
            if i > 1 then
                s_idx[i-1] = ("CREATE INDEX idx_state_token%d_id ON state(token%d_id)"):format(i, i)
            end
        end

        self.sql_create = sql_db_template:format(table_concat(s_rows, ",\n    "), names_list, table_concat(s_idx, ";\n"))
    end
end

function database:create_schema(order)
    self:_gen_sql(order, true)

    if self.db:exec(self.sql_create) ~= 0 then
        error(self.db:error_message())
    end

    self:set_config("order", tostring(order))
end

function database:begin_transaction()
    if self.db:exec "BEGIN IMMEDIATE" ~= 0 then
        error(self.db:error_message())
    end
end

function database:commit()
    if self.db:exec "COMMIT" ~= 0 then
        error(self.db:error_message())
    end
end


local function st_exec_0(db, st)
    if st:step() ~= sqlite.DONE then
        error(db:error_message())
    end
end

local function st_exec_1(db, st, mode)
    local res = st:step()
    if res == sqlite.ROW then
        if mode == nil then
            return st:get_uvalues()
        elseif mode then
            return st:get_named_values()
        else
            return st:get_values()
        end
    elseif res ~= sqlite.DONE then
        error(db:error_message())
    end
end

function database:exec_0(sql, ...)
    local st = self.prepare[sql]
    st:reset()
    st:bind_values(...)
    return st_exec_0(self.db, st)
end

function database:exec_t0(sql, tbl)
    local st = self.prepare[sql]
    st:reset()
    st:bind_names(tbl)
    return st_exec_0(self.db, st)
end

function database:exec_1u(sql, ...)
    local st = self.prepare[sql]
    st:reset()
    st:bind_values(...)
    return st_exec_1(self.db, st)
end

function database:exec_1a(sql, ...)
    local st = self.prepare[sql]
    st:reset()
    st:bind_values(...)
    return st_exec_1(self.db, st, false)
end

function database:exec_1t(sql, ...)
    local st = self.prepare[sql]
    st:reset()
    st:bind_values(...)
    return st_exec_1(self.db, st, true)
end

function database:exec_n(sql, ...)
    local st = self.prepare[sql]
    st:reset()
    st:bind_values(...)
    local list = {}
    local n = 1
    for row in st:nrows() do
        list[n] = row
        n = n + 1
    end
    return list
end

function database:table_exists(name)
    return self:exec_1u("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", name) == 1
end

function database:get_stats()
    return self:exec_1t "SELECT (SELECT count(*) FROM token) AS tokens, (SELECT count(*) FROM state) AS states, (SELECT count(*) FROM next_token) AS transitions"
end

function database:get_config(key)
    return self:exec_1u("SELECT val FROM markov_config WHERE key = ?", key)
end

function database:set_config(key, val)
    return self:exec_0("INSERT OR REPLACE INTO markov_config (key, val) VALUES (?, ?)", key, val)
end

-- token --

function database:get_token(id)
    return self:exec_1t("SELECT * FROM token WHERE id = ?", id)
end

function database:get_token_list(...)
    return self:exec_n(self.sql_get_token_list, ...)
end

function database:find_token_id(pos, text)
    return self:exec_1u("SELECT id FROM token WHERE pos = ? AND text = ?", pos, text)
end

function database:find_token(pos, text)
    return self:exec_1t("SELECT * FROM token WHERE pos = ? AND text = ?", pos, text)
end

function database:new_token(obj)
    self:exec_t0("INSERT INTO token (pos, text) VALUES ($pos, $text)", obj)
    return self.db:last_insert_rowid()
end

function database:inc_token(id)
    return self:exec_0("UPDATE token SET count = count + 1 WHERE id = $id", id)
end

-- state --

function database:get_state(id)
    return self:exec_1a(self.sql_get_state, id)
end

function database:find_state_id(...)
    return self:exec_1u(self.sql_find_state_id, ...)
end

function database:new_state(...)
    self:exec_0(self.sql_new_state, ...)
    return self.db:last_insert_rowid()
end

function database:state_max_id()
    return self:exec_1u "SELECT max(id) FROM state"
end

function database:random_state_with(token_id)
    return self:exec_1u(self.sql_random_state_with, token_id)
end

-- next_token --

function database:find_transition_next(state_id, token_id)
    return self:exec_1u("SELECT id FROM next_token WHERE state_id = ? AND token_id = ?", state_id, token_id)
end

function database:new_transition_next(state_id, token_id)
    return self:exec_0("INSERT INTO next_token (state_id, token_id) VALUES (?, ?)", state_id, token_id)
end

function database:inc_transition_next(id)
    return self:exec_0("UPDATE next_token SET count = count + 1 WHERE id = ?", id)
end

function database:random_transition_next(state_id)
    return self:exec_1u("SELECT token_id FROM next_token WHERE state_id = ? ORDER BY random_weighted(count) DESC LIMIT 1", state_id)
end

-- prev_token --

function database:find_transition_prev(state_id, token_id)
    return self:exec_1u("SELECT id FROM prev_token WHERE state_id = ? AND token_id = ?", state_id, token_id)
end

function database:new_transition_prev(state_id, token_id)
    return self:exec_0("INSERT INTO prev_token (state_id, token_id) VALUES (?, ?)", state_id, token_id)
end

function database:inc_transition_prev(id)
    return self:exec_0("UPDATE prev_token SET count = count + 1 WHERE id = ?", id)
end

function database:random_transition_prev(state_id)
    return self:exec_1u("SELECT token_id FROM prev_token WHERE state_id = ? ORDER BY random_weighted(count) DESC LIMIT 1", state_id)
end

return database
