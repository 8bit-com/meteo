local log = require('log')
local crud = require('crud')
local function init_spaces()
    local weather = box.schema.space.create(
        "weather",
        {
            format = {
                { name = "coordinates", type = "string", is_nullable = false },
                { name = "temperature", type = "string", is_nullable = false },
                { name = "bucket_id", type = "unsigned" },
            },
            if_not_exists = true,
        }
    )
    weather:create_index("coordinates", {
        parts = { { field = "coordinates" } },
        if_not_exists = true,
    })
    weather:create_index("bucket_id", {
        parts = { { field = "bucket_id" } },
        if_not_exists = true, unique = false,
    })
end

local function put(coordinates, temperature, bucket_id)
    local sql_query = string.format([[
    INSERT INTO "weather"
    VALUES ('%s', '%s', %d);
    ]], coordinates, temperature, bucket_id)
    local temp, err = box.execute(sql_query)
    if err ~= nil then
        log.info(err)
    end
    return temp
end

local function get(coordinates)
    local sql_query = string.format([[
    SELECT "temperature"
    FROM "weather"
    WHERE "coordinates" = '%s'
    LIMIT 1;
    ]], coordinates)
    local temp, err = box.execute(sql_query)
    if err ~= nil then
        log.info(err)
    end
    temp = crud.unflatten_rows(temp.rows, temp.metadata)
    return temp
end

local exported_functions = {
    get = get,
    put = put,
}

local function init(opts)
    if opts.is_master then
        init_spaces()
    end
    rawset(_G, "ddl", { get_schema = require("ddl").get_schema })

    for name, func in pairs(exported_functions) do
        rawset(_G, name, func)
    end
    return true
end

return {
    role_name = 'app.roles.cache',
    init = init,

    dependencies = {
        'cartridge.roles.vshard-storage',
    },
}
