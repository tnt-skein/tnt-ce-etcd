--- Клиент etcd поверх HTTP-шлюза API v3.
---
--- Читает ключи через /v3/kv/range и при необходимости берёт токен через
--- /v3/auth/authenticate. Ключи и значения в API v3 передаются в base64,
--- поэтому кодирование и разбор спрятаны здесь.
---
--- Модуль ничего не знает о конфигурации Tarantool: на входе адреса и
--- параметры, на выходе строка значения либо ошибка. Так его можно
--- проверить без живого кластера.

local digest = require('digest')
local json = require('json')

---@class TntCeEtcdFetchMeta
---@field endpoint string Адрес, который ответил
---@field revision number|nil Ревизия всего хранилища в миг ответа — только для отчёта
---@field mod_revision number|nil Ревизия самого ключа: его последняя правка
---@field missing boolean|nil Ключа нет — это не отказ связи

---@class TntCeEtcdClient
---@field _http table Транспорт HTTP
---@field _timeout number Таймаут запроса по умолчанию
local Module = {}
Module.__index = Module

--- Таймаут запроса, если не задан явно.
local DEFAULT_TIMEOUT = 5

--- Кодирует строку в base64 без переносов: того требует API etcd.
---@param value string
---@return string
local function encode_base64(value)
    return digest.base64_encode(value, { nowrap = true, urlsafe = false, nopad = false })
end

--- Раскодирует значение из ответа etcd.
---@param value string|nil
---@return string|nil
local function decode_base64(value)
    if value == nil then
        return nil
    end

    return digest.base64_decode(value)
end

--- Тело запроса на чтение одного ключа. Без range_end etcd отдаёт
--- ровно этот ключ, а не диапазон.
---@param key string
---@return string
local function build_range_body(key)
    return json.encode({ key = encode_base64(key) })
end

--- Создаёт клиента.
---@param opts { http_client: table|nil, timeout: number|nil }|nil
---@return TntCeEtcdClient
function Module.new(opts)
    opts = opts or {}

    return setmetatable({
        -- Транспорт вынесен в параметр: тесты подставляют двойник вместо
        -- настоящих сетевых запросов.
        _http = opts.http_client or require('http.client').new(),
        _timeout = opts.timeout or DEFAULT_TIMEOUT,
    }, Module)
end

---@class TntCeEtcdRequestOptions
---@field token string|nil Токен доступа
---@field ssl TntCeEtcdSslOptions|nil Параметры TLS
---@field timeout number|nil Срок запроса в секундах
---@field unix_socket string|nil Unix-сокет вместо сети
---@field interface string|nil Исходящий сетевой интерфейс
---@field verbose boolean|nil Подробный вывод curl на stderr

--- Параметры TLS — поля `config.etcd.ssl` схемы ядра под теми же именами.
---@class TntCeEtcdSslOptions
---@field ca_file string|nil Файл доверенных центров
---@field ca_path string|nil Каталог доверенных центров
---@field ssl_cert string|nil Клиентский сертификат
---@field ssl_key string|nil Ключ клиентского сертификата
---@field verify_peer boolean|nil Проверять ли сертификат сервера
---@field verify_host boolean|nil Проверять ли имя в сертификате

--- Выполняет запрос к одному адресу.
---@param endpoint string Базовый адрес etcd
---@param path string Путь запроса
---@param body string Тело запроса
---@param options TntCeEtcdRequestOptions|nil
---@return table|nil parsed Разобранный ответ
---@return string|nil err Причина отказа
function Module:request(endpoint, path, body, options)
    options = options or {}

    local request_options = {
        timeout = options.timeout or self._timeout,
        headers = { ['content-type'] = 'application/json' },
        -- Настройки запроса из config.etcd.http.request идут в http.client
        -- как есть: у него они зовутся так же, и это те же ручки curl.
        unix_socket = options.unix_socket,
        interface = options.interface,
        verbose = options.verbose,
    }

    if options.token ~= nil then
        request_options.headers['authorization'] = options.token
    end

    -- Параметры TLS применимы только к https-адресам.
    if endpoint:match('^https://') ~= nil and type(options.ssl) == 'table' then
        local ssl = options.ssl

        -- Признаки проверки уходят булевыми и нетронутыми: http.client
        -- ядра смотрит на истинность, и число 0 для него — «проверять»
        -- (сверено на 3.8: `verify_host = 0` отвергал чужое имя в
        -- сертификате, `verify_host = false` — пропускал). Незаданный
        -- признак — умолчание клиента, то есть проверка включена; так же
        -- разделы читает и Enterprise, поэтому `verify_peer: false` сам по
        -- себе имя в сертификате не отключает — на это есть `verify_host`.
        request_options.ca_file = ssl.ca_file
        request_options.ca_path = ssl.ca_path
        request_options.ssl_cert = ssl.ssl_cert
        request_options.ssl_key = ssl.ssl_key
        request_options.verify_peer = ssl.verify_peer
        request_options.verify_host = ssl.verify_host
    end

    local url = endpoint .. path

    -- Через pcall: не всякий отказ http.client отдаёт ответом с кодом.
    -- Сорванное рукопожатие TLS, сброс соединения, негодный файл
    -- сертификата libcurl бросает — сверено на 3.8: etcd, требующий
    -- клиентский сертификат, без него давал бросок «curl: Failure when
    -- receiving data from the peer». Пропущенный наверх, бросок обходил
    -- и следующий адрес списка, и откат на снимок.
    local called, response = pcall(self._http.request, self._http, 'POST', url, body, request_options)

    if not called then
        return nil, ('%s: %s'):format(url, tostring(response))
    end

    if response == nil then
        return nil, ('%s: ответа нет'):format(url)
    end

    if response.status >= 400 then
        return nil, ('%s: HTTP %d: %s'):format(url, response.status, tostring(response.body or ''))
    end

    local ok, parsed = pcall(json.decode, response.body or '')
    if not ok or type(parsed) ~= 'table' then
        return nil, ('%s: ответ не разобран как JSON: %s'):format(url, tostring(parsed))
    end

    ---@cast parsed table
    return parsed, nil
end

--- Получает токен доступа. etcd отзывает токены при смене учётных данных,
--- поэтому токен берётся заново на каждое чтение, а не кешируется.
---@param endpoint string
---@param username string
---@param password string|nil
---@param options table|nil
---@return string|nil token
---@return string|nil err
function Module:authenticate(endpoint, username, password, options)
    local body = json.encode({ name = username, password = password })

    local parsed, err = self:request(endpoint, '/v3/auth/authenticate', body, options)
    if parsed == nil then
        return nil, err
    end

    if parsed.token == nil then
        return nil, ('%s: etcd не вернул токен'):format(endpoint)
    end

    return parsed.token, nil
end

---@class TntCeEtcdFetchOptions
---@field username string|nil Пользователь etcd; без него вход не делается
---@field password string|nil Пароль
---@field ssl TntCeEtcdSslOptions|nil Параметры TLS
---@field timeout number|nil Срок запроса в секундах
---@field unix_socket string|nil Unix-сокет вместо сети
---@field interface string|nil Исходящий сетевой интерфейс
---@field verbose boolean|nil Подробный вывод curl на stderr

--- Читает ключ, перебирая адреса по порядку до первого успешного.
--- Простое переключение вместо агрессивной ротации: на старте кластера
--- одна попытка на адрес предсказуемее.
---@param endpoints string[]
---@param key string
---@param options TntCeEtcdFetchOptions|nil
---@return string|nil value Значение ключа
---@return string|nil err Причина отказа либо nil
---@return TntCeEtcdFetchMeta|nil meta
function Module:fetch(endpoints, key, options)
    options = options or {}

    local failures = {}

    for _, endpoint in ipairs(endpoints) do
        local token, request_error

        if options.username ~= nil and options.username ~= '' then
            token, request_error = self:authenticate(endpoint, options.username, options.password, options)
            if token == nil then
                table.insert(failures, ('%s: вход не удался: %s'):format(endpoint, tostring(request_error)))
                goto next_endpoint
            end
        end

        do
            local parsed, err = self:request(endpoint, '/v3/kv/range', build_range_body(key), {
                token = token,
                ssl = options.ssl,
                timeout = options.timeout,
                unix_socket = options.unix_socket,
                interface = options.interface,
                verbose = options.verbose,
            })

            if parsed == nil then
                table.insert(failures, ('%s: %s'):format(endpoint, tostring(err)))
                goto next_endpoint
            end

            local pairs_found = parsed.kvs
            if type(pairs_found) ~= 'table' or pairs_found[1] == nil then
                -- Ключа нет — это не отказ связи, а отсутствие данных.
                return nil, ('ключ %s не найден'):format(key), { endpoint = endpoint, missing = true }
            end

            local found = pairs_found[1]

            -- Ревизий две, и путать их нельзя. Ревизия хранилища растёт от
            -- любой записи в etcd — назначения лидера, замка, чужой правки
            -- рядом, — и узлы, прочитавшие одну и ту же конфигурацию в разные
            -- мгновения, разошлись бы ею на пустом месте. Версию конфигурации
            -- называет только ревизия самого ключа.
            return decode_base64(found.value),
                nil,
                {
                    endpoint = endpoint,
                    revision = parsed.header and tonumber(parsed.header.revision) or nil,
                    mod_revision = tonumber(found.mod_revision),
                }
        end

        ::next_endpoint::
    end

    return nil,
        ('ни один адрес etcd не ответил:\n  - %s'):format(table.concat(failures, '\n  - ')),
        nil
end

return Module
