--- Тесты клиента etcd. Сеть не используется: транспорт подменяется
--- двойником, который отдаёт заранее заготовленные ответы.

local digest = require('digest')
local json = require('json')
local t = require('luatest')

local helper = dofile('test/helper.lua')
local testing = helper.testing

local g = t.group('tnt.ce.etcd.client')

local MODULES = helper.CLIENT

g.after_each(function()
    testing.unload_sources(MODULES)
end)

---@class RecordedRequest
---@field method string
---@field url string
---@field body string
---@field opts table

--- Запросы, которые клиент отправил двойнику.
---@type RecordedRequest[]
local requests = {}

--- Запрос под указанным номером; падает, если его не сделали.
---@param index integer
---@return RecordedRequest
local function request_at(index)
    return (assert(requests[index], ('запрос №%d не отправлен'):format(index)))
end

--- Кодирует значение так же, как это делает etcd в ответах.
---@param value string
---@return string
local function as_etcd_value(value)
    return digest.base64_encode(value, { nowrap = true })
end

--- Собирает клиента с двойником транспорта.
---@param responder fun(method: string, url: string, body: string, opts: table): table|nil
---@return table
local function build_client(responder)
    requests = {}

    local client_module = testing.load_sources(MODULES, 'tnt.ce.etcd.client')

    return client_module.new({
        http_client = {
            request = function(_, method, url, body, opts)
                table.insert(requests, { method = method, url = url, body = body, opts = opts })
                return responder(method, url, body, opts)
            end,
        },
    })
end

--- Двойник, который отвечает на вход заданным образом, а на чтение —
--- успешно. Повторялся в нескольких проверках аутентификации.
---@param auth_response table Что вернуть на /v3/auth/authenticate
---@return table client
local function build_client_with_auth(auth_response)
    return build_client(function(_, url)
        if url:find('authenticate', 1, true) then
            return auth_response
        end
        return {
            status = 200,
            body = json.encode({ header = { revision = '1' }, kvs = { { value = as_etcd_value('ok') } } }),
        }
    end)
end

--- Ответ etcd с одним найденным ключом.
---
--- Ключ правили раньше, чем хранилище ответило: ревизия ключа меньше
--- ревизии хранилища, как и бывает в etcd, где пишут не только его.
---@param value string
---@param revision integer Ревизия хранилища
---@param mod_revision integer|nil Ревизия ключа; по умолчанию на единицу меньше
---@return table
local function found_response(value, revision, mod_revision)
    return {
        status = 200,
        body = json.encode({
            header = { revision = tostring(revision) },
            kvs = { { value = as_etcd_value(value), mod_revision = tostring(mod_revision or revision - 1) } },
        }),
    }
end

-- Значение ключа возвращается раскодированным.
g.test_fetch_returns_decoded_value = function()
    local client = build_client(function()
        return found_response('groups: {}', 42)
    end)

    local value, err, meta = client:fetch({ 'http://etcd:2379' }, '/cfg')

    t.assert_equals(err, nil)
    t.assert_equals(value, 'groups: {}')
    t.assert_equals(meta.endpoint, 'http://etcd:2379')
end

-- Ревизий в ответе две: хранилища и самого ключа. Версию конфигурации
-- называет вторая — первую двигает любая запись в etcd.
g.test_fetch_tells_both_revisions_apart = function()
    local client = build_client(function()
        return found_response('groups: {}', 42, 17)
    end)

    local _, _, meta = client:fetch({ 'http://etcd:2379' }, '/cfg')

    t.assert_equals(meta.revision, 42)
    t.assert_equals(meta.mod_revision, 17)
end

-- Ключ передаётся в base64, как того требует API v3.
g.test_fetch_encodes_key = function()
    local client = build_client(function()
        return found_response('x', 1)
    end)

    client:fetch({ 'http://etcd:2379' }, '/cfg/all')

    local sent = json.decode(request_at(1).body)
    t.assert_equals(digest.base64_decode(sent.key), '/cfg/all')
    t.assert_equals(request_at(1).url, 'http://etcd:2379/v3/kv/range')
    -- Тип тела etcd читает из заголовка: имя и значение сверяются дословно.
    t.assert_equals(request_at(1).opts.headers, { ['content-type'] = 'application/json' })
end

-- Отсутствие ключа отличается от отказа связи признаком missing.
g.test_missing_key_is_marked = function()
    local client = build_client(function()
        return { status = 200, body = json.encode({ header = { revision = '1' }, kvs = {} }) }
    end)

    local value, err, meta = client:fetch({ 'http://etcd:2379' }, '/cfg')

    t.assert_equals(value, nil)
    t.assert_str_contains(err, 'не найден')
    t.assert_equals(meta.missing, true)
end

-- Пустое значение etcd не кодирует вовсе: в ответе есть ключ, но нет
-- поля value. Такой ключ существует, и признака «не найден» у него нет —
-- иначе пустая конфигурация выглядела бы как отсутствующая.
g.test_key_with_an_empty_value_is_found = function()
    local client = build_client(function()
        return {
            status = 200,
            body = json.encode({ header = { revision = '7' }, kvs = { { key = 'L2NmZw==' } } }),
        }
    end)

    local value, err, meta = client:fetch({ 'http://etcd:2379' }, '/cfg')

    t.assert_equals(value, nil)
    t.assert_equals(err, nil)
    t.assert_equals(meta.missing, nil)
    t.assert_equals(meta.revision, 7)
    t.assert_equals(
        meta.mod_revision,
        nil,
        'ключ без ревизии в ответе — ревизия неизвестна'
    )
end

-- Адреса перебираются по порядку до первого ответившего.
g.test_fetch_tries_endpoints_in_order = function()
    local client = build_client(function(_, url)
        if url:find('первый', 1, true) then
            return { status = 500, body = 'сломан' }
        end
        return found_response('ok', 7)
    end)

    local value = client:fetch({ 'http://первый:2379', 'http://второй:2379' }, '/cfg')

    t.assert_equals(value, 'ok')
    t.assert_equals(#requests, 2, 'первый адрес тоже опрашивался')
end

-- Если не ответил ни один адрес, в ошибке перечислены все.
g.test_all_endpoints_failed_lists_every_error = function()
    local client = build_client(function()
        return { status = 503, body = 'недоступен' }
    end)

    local value, err = client:fetch({ 'http://a:2379', 'http://b:2379' }, '/cfg')

    -- Отказ сверяется целиком: разметку списка видно только между двумя
    -- записями, поэтому отказавших адресов два.
    t.assert_equals(value, nil)
    t.assert_equals(
        err,
        'ни один адрес etcd не ответил:\n'
            .. '  - http://a:2379: http://a:2379/v3/kv/range: HTTP 503: недоступен\n'
            .. '  - http://b:2379: http://b:2379/v3/kv/range: HTTP 503: недоступен'
    )
end

-- Бросок транспорта — отказ адреса, а не всего чтения: libcurl бросает
-- на сорванном рукопожатии TLS, и следующий адрес обязан получить свой
-- черёд.
g.test_transport_exception_moves_on_to_the_next_endpoint = function()
    local client = build_client(function(_, url)
        if url:find('первый', 1, true) then
            error('curl: Failure when receiving data from the peer', 0)
        end

        return found_response('ok', 7)
    end)

    local value, err, meta = client:fetch({ 'http://первый:2379', 'http://второй:2379' }, '/cfg')

    t.assert_equals({ value, err }, { 'ok', nil })
    t.assert_equals(meta.endpoint, 'http://второй:2379')
    t.assert_equals(#requests, 2)
end

-- Причину броска отказ называет словами транспорта, при адресе запроса.
g.test_transport_exception_is_named_in_the_error = function()
    local client = build_client(function()
        error('curl: SSL connect error', 0)
    end)

    local value, err, meta = client:fetch({ 'http://a:2379' }, '/cfg')

    t.assert_equals({ value, meta }, { nil, nil })
    t.assert_equals(
        err,
        'ни один адрес etcd не ответил:\n  - http://a:2379: http://a:2379/v3/kv/range: curl: SSL connect error'
    )
end

-- Бросок на входе — такой же отказ входа, как ответ с ошибкой.
g.test_transport_exception_on_authentication_is_reported = function()
    local client = build_client(function()
        error('curl: SSL connect error', 0)
    end)

    local token, err = client:authenticate('https://etcd:2379', 'root', 'secret')

    t.assert_equals(token, nil)
    t.assert_equals(err, 'https://etcd:2379/v3/auth/authenticate: curl: SSL connect error')
end

-- Пустой ответ транспорта не роняет клиента.
g.test_nil_response_is_an_error = function()
    local client = build_client(function()
        return nil
    end)

    local value, err = client:fetch({ 'http://etcd:2379' }, '/cfg')

    t.assert_equals(value, nil)
    t.assert_str_contains(err, 'ответа нет')
end

-- Неразбираемый JSON не роняет клиента.
-- Ответ, разобравшийся не в таблицу, ответом etcd не является.
g.test_json_that_is_not_an_object_is_an_error = function()
    local client = build_client(function()
        return { status = 200, body = '1' }
    end)

    local value, err = client:fetch({ 'http://etcd:2379' }, '/cfg')

    t.assert_equals(value, nil)
    t.assert_str_contains(err, 'не разобран как JSON')
end

g.test_broken_json_is_an_error = function()
    local client = build_client(function()
        return { status = 200, body = 'не json' }
    end)

    local value, err = client:fetch({ 'http://etcd:2379' }, '/cfg')

    t.assert_equals(value, nil)
    t.assert_str_contains(err, 'не разобран как JSON')
end

-- При заданном пользователе клиент сначала берёт токен и потом шлёт его.
g.test_authenticates_before_fetch = function()
    local client = build_client(function(_, url)
        if url:find('authenticate', 1, true) then
            return { status = 200, body = json.encode({ token = 'токен-42' }) }
        end
        return found_response('ok', 1)
    end)

    client:fetch({ 'http://etcd:2379' }, '/cfg', { username = 'admin', password = 'секрет' })

    t.assert_equals(#requests, 2)
    t.assert_str_contains(request_at(1).url, '/v3/auth/authenticate')
    t.assert_equals(request_at(2).opts.headers['authorization'], 'токен-42')
end

-- Отказ входа не выдаётся за отсутствие ключа, и причина видна.
g.test_failed_authentication_is_reported = function()
    local client = build_client_with_auth({ status = 401, body = 'неверный пароль' })

    local value, err = client:fetch({ 'http://etcd:2379' }, '/cfg', { username = 'admin' })

    t.assert_equals(value, nil)
    t.assert_str_contains(err, 'вход не удался')
    t.assert_str_contains(
        err,
        'неверный пароль',
        'причина отказа доходит до вызывающего'
    )
    t.assert_str_contains(err, '401')
end

-- Причина отказа входа возвращается и напрямую из authenticate.
g.test_authenticate_returns_reason = function()
    local client = build_client(function()
        return { status = 403, body = 'учётка заблокирована' }
    end)

    local token, err = client:authenticate('http://etcd:2379', 'admin', 'секрет')

    t.assert_equals(token, nil)
    t.assert_str_contains(err, 'учётка заблокирована')
end

-- etcd без токена в ответе — тоже отказ входа.
g.test_missing_token_is_reported = function()
    local client = build_client_with_auth({ status = 200, body = json.encode({}) })

    local value, err = client:fetch({ 'http://etcd:2379' }, '/cfg', { username = 'admin' })

    t.assert_equals(value, nil)
    t.assert_str_contains(err, 'не вернул токен')
end

-- Параметры TLS применяются только к адресам https.
g.test_tls_options_apply_to_https_only = function()
    local client = build_client(function()
        return found_response('ok', 1)
    end)

    client:fetch({ 'http://plain:2379' }, '/cfg', { ssl = { ca_file = '/ca.pem' } })
    t.assert_equals(request_at(1).opts.ca_file, nil, 'по http настройки TLS не нужны')

    local secure_client = build_client(function()
        return found_response('ok', 1)
    end)
    secure_client:fetch({ 'https://secure:2379' }, '/cfg', { ssl = { ca_file = '/ca.pem' } })
    t.assert_equals(request_at(1).opts.ca_file, '/ca.pem')
end

-- Раздел config.etcd.ssl доходит до транспорта под именами схемы ядра:
-- ровно теми, что знает и http.client.
g.test_tls_options_are_passed_under_the_kernel_names = function()
    local client = build_client(function()
        return found_response('ok', 1)
    end)

    client:fetch({ 'https://secure:2379' }, '/cfg', {
        ssl = {
            ca_file = '/ca.pem',
            ca_path = '/etc/ssl/certs',
            ssl_cert = '/client.pem',
            ssl_key = '/client.key',
            verify_peer = true,
            verify_host = false,
        },
    })

    local opts = request_at(1).opts
    t.assert_equals(opts.ca_file, '/ca.pem')
    t.assert_equals(opts.ca_path, '/etc/ssl/certs')
    t.assert_equals(opts.ssl_cert, '/client.pem')
    t.assert_equals(opts.ssl_key, '/client.key')
    t.assert_equals(opts.verify_peer, true)
    t.assert_equals(opts.verify_host, false)
end

-- Незаданные признаки проверки транспорту не подсказываются: его
-- умолчание — проверять, и подсказка числом это умолчание не выключила
-- бы, а булевой — подменила бы то, что решил оператор.
g.test_unset_tls_verification_is_left_to_the_transport = function()
    local client = build_client(function()
        return found_response('ok', 1)
    end)

    client:fetch({ 'https://secure:2379' }, '/cfg', { ssl = { ca_file = '/ca.pem' } })

    t.assert_equals(request_at(1).opts.verify_peer, nil)
    t.assert_equals(request_at(1).opts.verify_host, nil)
end

-- Проверку сертификата можно выключить явно, и она уходит булевым
-- `false`: http.client смотрит на истинность, а 0 для него — «проверять».
-- Имя в сертификате при этом не трогается: у него свой признак.
g.test_tls_verification_can_be_disabled = function()
    local client = build_client(function()
        return found_response('ok', 1)
    end)

    client:fetch({ 'https://secure:2379' }, '/cfg', { ssl = { verify_peer = false } })

    t.assert_equals(request_at(1).opts.verify_peer, false)
    t.assert_equals(request_at(1).opts.verify_host, nil)
end

-- Настройки запроса из config.etcd.http.request доходят до транспорта
-- под своими именами — и до чтения ключа, и до входа.
g.test_request_options_are_passed_to_the_transport = function()
    local client = build_client(function(_, url)
        if url:find('authenticate', 1, true) then
            return { status = 200, body = json.encode({ token = 'токен' }) }
        end
        return found_response('ok', 1)
    end)

    client:fetch({ 'http://etcd:2379' }, '/cfg', {
        username = 'admin',
        unix_socket = '/run/etcd.sock',
        interface = 'eth1',
        verbose = true,
    })

    for index = 1, 2 do
        local opts = request_at(index).opts
        t.assert_equals(opts.unix_socket, '/run/etcd.sock', 'запрос №' .. index)
        t.assert_equals(opts.interface, 'eth1', 'запрос №' .. index)
        t.assert_equals(opts.verbose, true, 'запрос №' .. index)
    end
end

-- Незаданные настройки запроса транспорту не передаются вовсе: у него
-- свои умолчания, и nil их не перебивает.
g.test_unset_request_options_are_not_sent = function()
    local client = build_client(function()
        return found_response('ok', 1)
    end)

    client:fetch({ 'http://etcd:2379' }, '/cfg')

    local opts = request_at(1).opts
    t.assert_equals(opts.unix_socket, nil)
    t.assert_equals(opts.interface, nil)
    t.assert_equals(opts.verbose, nil)
end

-- Граница кода ответа: 400 и выше — отказ, ниже — успех.
g.test_status_boundary = function()
    -- 399 обязан считаться успехом: иначе сдвиг границы вниз остался бы
    -- незамеченным.
    local client = build_client(function()
        return { status = 399, body = require('json').encode({ kvs = { { value = 'eA==' } } }) }
    end)
    t.assert_equals(client:fetch({ 'http://etcd:2379' }, '/cfg'), 'x', 'код 399 — ещё успех')

    for _, status in ipairs({ 400, 401, 500 }) do
        local failing = build_client(function()
            return { status = status, body = 'отказ' }
        end)

        local value, err = failing:fetch({ 'http://etcd:2379' }, '/cfg')

        t.assert_equals(value, nil, 'код ' .. status)
        t.assert_str_contains(err, tostring(status))
    end

    local ok_client = build_client(function()
        return found_response('ok', 1)
    end)
    t.assert_equals(ok_client:fetch({ 'http://etcd:2379' }, '/cfg'), 'ok', 'код 200 — успех')
end

-- Тело ответа попадает в текст ошибки: без него причину не понять.
g.test_error_includes_response_body = function()
    local client = build_client(function()
        return { status = 403, body = 'доступ запрещён' }
    end)

    local _, err = client:fetch({ 'http://etcd:2379' }, '/cfg')

    t.assert_str_contains(err, 'доступ запрещён')
end

-- Ключ кодируется обычным base64: с выравниванием, без переносов и без
-- URL-алфавита. Иначе etcd не найдёт ключ.
g.test_key_encoding_matches_etcd_expectations = function()
    local client = build_client(function()
        return found_response('ok', 1)
    end)

    -- Длинный ключ выявляет переносы строк, а символы ?> — подмену алфавита.
    local key = '/' .. string.rep('очень-длинный-путь/', 6) .. 'ключ?>'
    client:fetch({ 'http://etcd:2379' }, key)

    local encoded = json.decode(request_at(1).body).key
    t.assert_equals(encoded:find('\n'), nil, 'переносов быть не должно')
    t.assert_equals(digest.base64_decode(encoded), key)
    t.assert_equals(
        encoded:find('[^A-Za-z0-9+/=]'),
        nil,
        'алфавит обычный, не URL-безопасный'
    )
    t.assert_str_contains(encoded, '=', 'выравнивание сохраняется')
end

-- Без таймаута в параметрах берётся умолчание клиента.
g.test_default_timeout_is_applied = function()
    local client = build_client(function()
        return found_response('ok', 1)
    end)

    client:fetch({ 'http://etcd:2379' }, '/cfg')

    t.assert_equals(request_at(1).opts.timeout, 5)
end

-- Умолчание можно задать при создании клиента.
g.test_client_timeout_can_be_configured = function()
    requests = {}
    local client_module = testing.load_sources(MODULES, 'tnt.ce.etcd.client')
    local client = client_module.new({
        timeout = 23,
        http_client = {
            request = function(_, method, url, body, opts)
                table.insert(requests, { method = method, url = url, body = body, opts = opts })
                return found_response('ok', 1)
            end,
        },
    })

    client:fetch({ 'http://etcd:2379' }, '/cfg')

    t.assert_equals(request_at(1).opts.timeout, 23)
end

-- Таймаут из параметров доходит до транспорта.
g.test_timeout_is_passed_through = function()
    local client = build_client(function()
        return found_response('ok', 1)
    end)

    client:fetch({ 'http://etcd:2379' }, '/cfg', { timeout = 17 })

    t.assert_equals(request_at(1).opts.timeout, 17)
end
