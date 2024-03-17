local vshard = require('vshard')
local cartridge = require('cartridge')
local json = require('json')
local log = require('log')

local function setTemperature(coordinates, temperature, bucket_id)
    local res = vshard.router.call(bucket_id, 'write', 'put', {coordinates, temperature, bucket_id })
    return res
end

local function getTemperature(coordinates, bucket_id)
    local res = vshard.router.call(bucket_id, 'read', 'get', {coordinates})
    return res
end

local function httpRequest(latitude, longitude)
    local http_client = require('http.client').new()
    local response = http_client:post('https://api.open-meteo.com/v1/forecast', nil, {
        params = { latitude = latitude,
                   longitude = longitude,
                   current_weather = true },
    })
    local body = json.decode(response.body)
    return body.current_weather.temperature
end

local function getData(latitude, longitude)
    local coordinates = latitude .. longitude
    local bucket_id = vshard.router.bucket_id_strcrc32(coordinates)
    local result = getTemperature(coordinates, bucket_id)
    if result[1] == nil then
        local temp = httpRequest(latitude, longitude)
        setTemperature(coordinates, temp, bucket_id)
        result = getTemperature(coordinates, bucket_id)
    end
    return result[1].temperature
end

local exported_functions = {
    getTemperature = getTemperature,
    setTemperature = setTemperature,
}

local function init(opts)
    if opts.is_master then
        box.schema.user.grant('guest',
            'read,write,execute',
            'universe',
            nil, { if_not_exists = true }
        )
    end
    for name, func in pairs(exported_functions) do
        rawset(_G, name, func)
    end

    local httpd = assert(cartridge.service_get('httpd'), "Failed to get httpd service")
    httpd:route({ method = 'GET', path = '/getWeather' }, function(req)
        local latitude = req:param("latitude")
        local longitude = req:param("longitude")
        local body = getData(latitude, longitude)
        return { status = 200, body = body }
    end)

    return true
end

return {
    role_name = 'app.roles.router',
    init = init,
    dependencies = { 'cartridge.roles.vshard-router', },
}
