--- Наблюдение на живом ядре 3.8: узел, поднятый на снимке при лежащем
--- etcd, сам сходит со снимка, когда хранилище начинает отвечать.
---
--- Двойник клиента показывает, что источник заводит наблюдателя и на
--- снимке и просит перечитать конфигурацию, но не главное: что
--- наблюдатель, заведённый ещё до `box.cfg`, переживает подъём узла,
--- а перечитывание, о котором он просит, ядро исполняет и снова зовёт
--- источник. Показать это может только настоящее ядро.
---
--- etcd здесь — двойник HTTP-шлюза на сокете проверки. Ему довольно
--- отвечать на чтение ключа, а поднять его можно ровно в тот миг, когда
--- проверке нужно «вернуть» хранилище: без докера и без общего порта.

local t = require('luatest')
local fio = require('fio')
local json = require('json')
local digest = require('digest')
local socket = require('socket')

local helper = dofile('test/helper.lua')
local testing = helper.testing

local g = t.group('tnt.ce.etcd.watch.live')

--- Имя инстанса.
local NAME = 'watch-001-a'

--- Префикс конфигурации в хранилище.
local PREFIX = '/tnt-watch'

--- Сколько двойник шлюза ждёт запроса целиком.
local READ_TIMEOUT = 5

--- Сколько узлу дано на то, чтобы сойти со снимка: такт наблюдения —
--- пять секунд, и ещё столько же — на перечитывание и запас машине.
local RECOVERY_TIMEOUT = 10

--- Каталог запуска узла, чей etcd молчит: адрес указывает на свободный
--- порт, и узел поднимается на снимке.
---@param port integer Порт, на котором позже заговорит двойник шлюза
---@return string launch
local function prepared(port)
    local launch = fio.tempdir()

    testing.write_file(fio.pathjoin(launch, 'config.yaml'), table.concat({
        'credentials: {users: {guest: {roles: [super]}}}',
        'config:',
        '  etcd:',
        ("    endpoints: ['http://127.0.0.1:%d']"):format(port),
        ('    prefix: %s'):format(PREFIX),
        '    http: {request: {timeout: 1}}',
        ('groups: {g: {replicasets: {r: {instances: {%s: {'):format(NAME),
        ("  iproto: {listen: [{uri: 'unix/:%s/instance.iproto'}]},"):format(launch),
        '}}}}}}',
    }, '\n') .. '\n')

    testing.write_file(fio.pathjoin(launch, 'init.lua'), "require('strict').on()\n_G.ready = true\n")
    testing.write_file(
        fio.pathjoin(launch, '.env'),
        'TNT_CE_EXTENSIONS=tnt.ce.etcd\nTNT_CE_ETCD_SNAPSHOT=снимок.yaml\n'
    )
    testing.write_file(fio.pathjoin(launch, 'снимок.yaml'), 'roles_cfg: {marker: snapshot}\n')

    return launch
end

--- Ответ шлюза на чтение ключа: значение и ревизия, если ключ есть.
---@param keys table<string, { value: string, revision: integer }>
---@param body string Тело запроса `/v3/kv/range`
---@return string
local function range_answer(keys, body)
    local key = digest.base64_decode(json.decode(body).key)
    local found = keys[key]
    local answer = { header = { revision = '100' } }

    -- Числа шлюз etcd отдаёт строками: они 64-битные, и клиент обязан
    -- читать их так же, как от настоящего хранилища.
    if found ~= nil then
        answer.kvs = {
            {
                key = digest.base64_encode(key, { nowrap = true }),
                value = digest.base64_encode(found.value, { nowrap = true }),
                mod_revision = tostring(found.revision),
            },
        }
    end

    return json.encode(answer)
end

--- Поднимает двойник HTTP-шлюза etcd: на чтение ключа отвечает его
--- значением и ревизией, на незнакомый ключ — «ключа нет».
---@param port integer
---@param keys table<string, { value: string, revision: integer }>
---@return table server
local function gateway(port, keys)
    local server = socket.tcp_server('127.0.0.1', port, function(connection)
        local head = connection:read('\r\n\r\n', READ_TIMEOUT)

        if head == nil or head == '' then
            return
        end

        local length = (tonumber(head:lower():match('content%-length:%s*(%d+)')) or 0) --[[@as integer]]
        local answer = range_answer(keys, connection:read(length, READ_TIMEOUT))

        connection:write(table.concat({
            'HTTP/1.1 200 OK',
            'Content-Type: application/json',
            ('Content-Length: %d'):format(#answer),
            'Connection: close',
            '',
            answer,
        }, '\r\n'))
    end)

    t.assert_not_equals(server, nil, 'двойник шлюза не поднят: порт занят')

    return server
end

--- Что узел знает о своей конфигурации: метка из неё, состояние
--- источника и ревизия, которую ядро считает применённой.
---@param server table
---@return table
local function observed(server)
    return server:exec(function()
        local extras = require('tnt.ce.extras')
        ---@type any
        local config = require('config')
        local status = extras.status().sources.etcd
        local active = config:info('v2').meta.active.etcd

        return {
            marker = config:get('roles_cfg.marker'),
            stale = status.stale,
            watching = status.watching,
            revision = status.revision,
            applied = active and active.revision or nil,
        }
    end)
end

g.after_each(function()
    if g.server ~= nil then
        g.server:drop()
        g.server = nil
    end

    if g.gateway ~= nil then
        g.gateway:close()
        g.gateway = nil
    end

    if g.launch ~= nil then
        fio.rmtree(g.launch)
        g.launch = nil
    end
end)

g.test_node_started_on_a_snapshot_leaves_it_once_etcd_answers = function()
    local port = testing.free_port()

    g.launch = prepared(port)
    g.server = helper.start_in(g.launch, NAME)

    -- etcd молчит: узел поднят на снимке и всё равно наблюдает.
    t.assert_equals(observed(g.server), { marker = 'snapshot', stale = true, watching = true })

    g.gateway = gateway(port, {
        [PREFIX .. '/config/all'] = { value = 'roles_cfg: {marker: etcd}\n', revision = 5 },
    })

    -- Хранилище заговорило: наблюдатель замечает это сам, без ручного
    -- `config:reload()`, и ядро применяет прочитанное.
    t.helpers.retrying({ timeout = RECOVERY_TIMEOUT, delay = 0.25 }, function()
        t.assert_equals(
            observed(g.server),
            { marker = 'etcd', stale = false, watching = true, revision = 5, applied = 5 }
        )
    end)
end
