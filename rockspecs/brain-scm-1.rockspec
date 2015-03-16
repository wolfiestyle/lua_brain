package = "brain"
version = "scm-1"

source = {
    url = "git://github.com/darkstalker/lua_brain.git",
}

description = {
    summary = "Chat bot engine based on Markov chains",
    detailed = [[
        Chat bot engine based on Markov chains.
    ]],
    homepage = "https://github.com/darkstalker/lua_brain",
    license = "MIT/X11",
}

dependencies = {
    "lua >= 5.1",
    "lsqlite3 >= 0.9.3",
    "utf8 >= 1.1",
}

build = {
    type = "builtin",
    modules = {
        brain = "src/brain.lua",
        ["brain.database"] = "src/brain/database.lua",
        ["brain.engine"] = "src/brain/engine.lua",
        ["brain.tokenizer"] = "src/brain/tokenizer.lua",
    },
    copy_directories = { "examples" },
}
