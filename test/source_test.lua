--- Тесты источника конфигурации. Ни etcd, ни сеть не нужны: клиент
--- подменяется двойником, снимок уводится во временный каталог.

local t = require('luatest')
local fio = require('fio')

local helper = dofile('test/helper.lua')
local testing = helper.testing

local g = t.group('tnt.ce.etcd.source')

--- Исходник источника: грузится после снимка, которого он берёт по имени.
local SOURCE = helper.SOURCE

--- Предупреждения, записанные источником.
---@type string[]
local warnings

--- Те же предупреждения с полями: по ним видно, какую причину назвал журнал.
---@type { message: string, fields: table|nil }[]
local warned

--- Ошибки, записанные источником, с полями.
---@type { message: string, fields: table|nil }[]
local errors

--- Опознаватели запроса, под которыми шли записи и перечитывания, по порядку.
---@type { what: string, request_id: string|nil }[]
local marks

---@class RecordedFetch
---@field key string
---@field endpoints string[]
---@field options table
---@field client_opts table

--- Обращения двойника клиента: ключ и переданные параметры.
---@type RecordedFetch[]
local fetches

--- Тела наблюдателей, отложенные вместо запуска фибером.
---@type function[]
local watchers

--- Сколько раз источник просил перечитать конфигурацию.
---@type number
local reloads

--- Отказ перечитывания, если задан.
---@type string|nil
local reload_error

--- Сколько раз наблюдатель уходил ждать.
---@type number
local naps

--- Окружение, которое видят источник и снимок; своё на каждую проверку.
---@type table<string, string>
local vars

--- Собирает источник с подменёнными клиентом и снимком.
---@param responder fun(key: string): string|nil, string|nil, table|nil
---@param overrides table|nil Настройки наблюдения; `files` — что лежит на диске для `tnt-env`
---@return table source
---@return string snapshot_path
local function build_source(responder, overrides)
    overrides = overrides or {}
    warnings = {}
    warned = {}
    errors = {}
    marks = {}
    fetches = {}
    watchers = {}
    reloads = 0
    reload_error = nil
    naps = 0

    local snapshot_path = fio.pathjoin(fio.tempdir(), 'snapshot.yaml')
    vars = { TNT_CE_ETCD_SNAPSHOT = snapshot_path }

    -- Снимок — из исходника и под подменённым окружением: источник берёт
    -- его по имени модуля, и установленная копия читала бы настоящее.
    helper.snapshot(vars, overrides.files)

    local source_module = testing.load_sources(SOURCE, 'tnt.ce.etcd.source')
    local context = testing.module('tnt.context')

    --- Отмечает, под каким опознавателем запроса случилось дело.
    ---@param what string
    local function mark(what)
        table.insert(marks, { what = what, request_id = context.get(context.REQUEST_ID) })
    end

    ---@type any
    local source

    source = source_module.new({
        client_factory = function(opts)
            return {
                fetch = function(_, endpoints, key, options)
                    table.insert(fetches, { key = key, endpoints = endpoints, options = options, client_opts = opts })
                    return responder(key)
                end,
            }
        end,
        logger = {
            warn = function(format, ...)
                mark('warn')
                table.insert(warnings, string.format(format, ...))
                table.insert(warned, { message = format, fields = (...) })
            end,
            info = function() end,
            error = function(message, fields)
                mark('error')
                table.insert(errors, { message = message, fields = fields })
            end,
        },

        watch_interval = overrides.watch_interval,

        -- Наблюдатель не запускается фибером: его тело откладывается,
        -- и проверка сама решает, когда сделать такт.
        spawn = function(body)
            table.insert(watchers, body)
        end,

        -- Ожидание останавливает наблюдателя со второго захода: первый
        -- проход нужен целиком, а бесконечный цикл в проверке не кончился
        -- бы никогда.
        sleep = overrides.sleep or function()
            mark('sleep')
            naps = naps + 1

            if naps > 1 then
                source:unwatch()
            end
        end,

        reload = overrides.reload or function()
            mark('reload')
            reloads = reloads + 1

            if reload_error ~= nil then
                error(reload_error)
            end
        end,
    })

    return source, snapshot_path
end

--- Конфигурация инстанса с разделом etcd.
---@param overrides table|nil
---@return table
local function iconfig(overrides)
    local etcd = {
        endpoints = { 'http://etcd:2379' },
        prefix = '/tnt',
    }

    for key, value in pairs(overrides or {}) do
        etcd[key] = value
    end

    return { config = { etcd = etcd } }
end

g.after_each(function()
    helper.forget_environment()
    testing.unload_sources(SOURCE)
    helper.forget_snapshot()
end)

-- Источник объявляет поля, которых требует ядро.
g.test_declares_source_contract = function()
    helper.assert_source_contract(build_source(function() end))
end

-- Прочитанная конфигурация разбирается и отдаётся ядру.
g.test_reads_configuration = function()
    local source = build_source(function()
        return 'groups:\n  g: {}\n', nil, { revision = 9, mod_revision = 5, endpoint = 'http://etcd:2379' }
    end)

    source:sync(nil, iconfig())

    t.assert_equals(source:get(), { groups = { g = {} } })
    -- Ревизия ключа, а не хранилища: ту двигает любая запись в etcd.
    t.assert_equals(source:status().revision, 5)
    t.assert_equals(source:status().stale, false)
end

-- ── Рассказ ядру ─────────────────────────────────────────────────────

--- Двойник модуля конфигурации: записывает, что ему рассказали.
---@return table config
---@return table[] told
local function kernel_double()
    local told = {}

    return {
        _meta = function(_, name, key, value)
            table.insert(told, { name, key, value })
        end,
    },
        told
end

-- Ревизия прочитанной конфигурации рассказывается ядру под именем
-- источника: так она попадает в config:info('v2').meta, и ядро само
-- отличит прочитанное (last) от применённого (active).
g.test_reports_revision_to_the_kernel = function()
    local source = build_source(function()
        return 'groups: {}', nil, { revision = 9, mod_revision = 5 }
    end)
    local config, told = kernel_double()

    source:sync(config, iconfig())

    t.assert_equals(told, { { 'etcd', 'revision', 5 } })
end

-- Под своим именем, а не под общим: ядро складывает сведения по источникам.
g.test_reports_under_its_own_name = function()
    local source = build_source(function()
        return 'groups: {}', nil, { mod_revision = 5 }
    end)
    source.name = 'store'
    local config, told = kernel_double()

    source:sync(config, iconfig())

    t.assert_equals(told, { { 'store', 'revision', 5 } })
end

-- Конфигурация по устаревшему ключу — тоже прочитанная, и ревизия у неё есть.
g.test_reports_revision_of_the_legacy_key = function()
    local source = build_source(function(key)
        if key == '/tnt/config/all' then
            return nil, 'ключ не найден', { missing = true }
        end
        return 'groups: {}', nil, { mod_revision = 6 }
    end)
    local config, told = kernel_double()

    source:sync(config, iconfig())

    t.assert_equals(told, { { 'etcd', 'revision', 6 } })
end

-- Ревизии ключа нет — рассказывать нечего: ядро приняло бы nil за
-- сведения, а ревизия хранилища версией конфигурации не является.
g.test_does_not_report_an_unknown_revision = function()
    local source = build_source(function()
        return 'groups: {}', nil, { revision = 9 }
    end)
    local config, told = kernel_double()

    source:sync(config, iconfig())

    t.assert_equals(told, {})
end

-- На снимке ревизия неизвестна, и ядру об этом не говорят: у конфигурации
-- со снимка ревизии нет, и выдумывать её ядру нельзя.
g.test_does_not_report_from_a_snapshot = function()
    local available = true
    local source = build_source(function()
        if available then
            return 'groups: {}', nil, { mod_revision = 5 }
        end
        return nil, 'etcd недоступен', nil
    end)
    local config, told = kernel_double()

    source:sync(config, iconfig())
    available = false
    source:sync(config, iconfig())

    t.assert_equals(source:status().stale, true)
    t.assert_equals(told, { { 'etcd', 'revision', 5 } }, 'рассказано один раз, при чтении')
end

-- Ядро без внутреннего метода — источник молчит, а не падает.
g.test_kernel_without_the_method_is_tolerated = function()
    local source = build_source(function()
        return 'groups: {}', nil, { mod_revision = 5 }
    end)

    source:sync({}, iconfig())

    t.assert_equals(source:status().revision, 5)
end

-- Читается канонический ключ, а не устаревший.
g.test_reads_canonical_key_first = function()
    local source = build_source(function()
        return 'groups: {}', nil, {}
    end)

    source:sync(nil, iconfig())

    t.assert_equals(assert(fetches[1]).key, '/tnt/config/all')
end

-- Завершающие слэши в префиксе не удваиваются в ключе.
g.test_prefix_slashes_are_trimmed = function()
    local source = build_source(function()
        return 'groups: {}', nil, {}
    end)

    source:sync(nil, iconfig({ prefix = '/tnt///' }))

    t.assert_equals(assert(fetches[1]).key, '/tnt/config/all')
end

-- Если канонического ключа нет, читается устаревший.
g.test_falls_back_to_legacy_key = function()
    local source = build_source(function(key)
        if key == '/tnt/config/all' then
            return nil, 'ключ не найден', { missing = true }
        end
        return 'groups:\n  legacy: {}\n', nil, {}
    end)

    source:sync(nil, iconfig())

    t.assert_equals(#fetches, 2)
    t.assert_equals(assert(fetches[2]).key, '/tnt/config')
    t.assert_equals(source:get(), { groups = { legacy = {} } })
end

-- Отсутствие обоих ключей — не отказ: кластер поднимется из других источников.
g.test_missing_keys_yield_empty_config = function()
    local source = build_source(function()
        return nil, 'ключ не найден', { missing = true }
    end)

    source:sync(nil, iconfig())

    t.assert_equals(source:get(), {})
    t.assert_equals(source:status().stale, false)
end

--- Ответ etcd, перед которым шла работа без уступки.
---
--- Отметка цикла событий стоит с уступки в начале ответа, а настоящие
--- часы уходят вперёд на всю работу.
---@param answer fun(key: string): string|nil, string|nil, table|nil
---@return fun(key: string): string|nil, string|nil, table|nil responder
---@return fun(): number began Настоящее время начала работы
local function after_work(answer)
    local clock = require('clock')

    ---@type number
    local began = 0

    return function(key)
        require('fiber').yield()
        began = clock.realtime()
        testing.work_without_yielding(0.02)

        return answer(key)
    end, function()
        return began
    end
end

-- Время чтения — настоящее время, а не время последней уступки: разбор
-- и работа без уступки перед ним отодвинули бы его назад.
g.test_read_time_is_the_real_time_of_the_read = function()
    local responder, began = after_work(function()
        return 'groups: {}', nil, {}
    end)
    local source = build_source(responder)

    source:sync(nil, iconfig())

    t.assert_ge(source:status().synced_at - began(), 0.019)
end

g.test_read_time_of_an_empty_configuration_is_the_real_time_too = function()
    local responder, began = after_work(function()
        return nil, 'ключ не найден', { missing = true }
    end)
    local source = build_source(responder)

    source:sync(nil, iconfig())

    t.assert_ge(source:status().synced_at - began(), 0.019)
end

-- Без раздела config.etcd источник молчит.
g.test_without_etcd_section_source_is_silent = function()
    local source = build_source(function()
        error('обращений к etcd быть не должно')
    end)

    source:sync(nil, { config = {} })

    t.assert_equals(source:get(), {})
    t.assert_equals(#fetches, 0)
end

-- Ядро зовёт источник и тогда, когда предыдущие источники ничего
-- не собрали: ни конфигурации инстанса, ни раздела config в ней. Это
-- не отказ, а порядок загрузки, и источнику просто нечего добавить.
g.test_sync_before_any_configuration_is_silent = function()
    local source = build_source(function()
        error('обращений к etcd быть не должно')
    end)

    source:sync(nil, nil)
    source:sync(nil, {})

    t.assert_equals(source:get(), {})
    t.assert_equals(#fetches, 0)
end

-- get отдаёт пустую таблицу, а не nil: слияние конфигураций отвергает nil.
g.test_get_returns_table_before_sync = function()
    local source = build_source(function() end)

    t.assert_equals(source:get(), {})
end

-- Пустой список адресов — ошибка конфигурации. Отказ сверяется целиком:
-- оператор читает его в выводе старта, и приписка места в коде
-- («source.lua:NN:») там только мешает.
g.test_empty_endpoints_are_rejected = function()
    local source = build_source(function() end)

    t.assert_error_msg_equals(
        '[tnt.ce.etcd] config.etcd.endpoints должен быть непустым списком',
        source.sync,
        source,
        nil,
        iconfig({ endpoints = {} })
    )
end

-- Префикс обязателен.
g.test_missing_prefix_is_rejected = function()
    local source = build_source(function() end)

    t.assert_error_msg_equals(
        '[tnt.ce.etcd] config.etcd.prefix обязателен',
        source.sync,
        source,
        nil,
        iconfig({ prefix = '' })
    )
end

-- Неразбираемая конфигурация из etcd — ошибка, а не тихий пропуск.
g.test_broken_yaml_is_rejected = function()
    local source = build_source(function()
        return 'это: [не: закрыто', nil, {}
    end)

    t.assert_error_msg_contains('не разобрана как YAML', source.sync, source, nil, iconfig())
end

-- Учётные данные и параметры TLS доходят до клиента.
g.test_credentials_are_passed_to_client = function()
    local source = build_source(function()
        return 'groups: {}', nil, {}
    end)

    source:sync(
        nil,
        iconfig({
            username = 'admin',
            password = 'секрет',
            ssl = { ca_file = '/ca.pem' },
            http = { request = { timeout = 11 } },
        })
    )

    local options = assert(fetches[1]).options
    t.assert_equals(options.username, 'admin')
    t.assert_equals(options.password, 'секрет')
    t.assert_equals(options.ssl.ca_file, '/ca.pem')
    t.assert_equals(options.timeout, 11)
end

-- ── Имена и записи схемы ядра ────────────────────────────────────────

-- Срок запроса записывается так же, как принимает ядро: строкой с
-- единицей. Сверено на 3.8: `timeout: 30s` проходит проверку схемы и
-- доходит до источника строкой — раньше она молча превращалась в умолчание.
g.test_duration_strings_are_read_like_the_kernel_reads_them = function()
    for _, case in ipairs({
        { written = '30s', seconds = 30 },
        { written = '2m', seconds = 120 },
        { written = '500ms', seconds = 0.5 },
        { written = 7, seconds = 7 },
        { written = '7', seconds = 7 },
    }) do
        local source = build_source(function()
            return 'groups: {}', nil, {}
        end)

        source:sync(nil, iconfig({ http = { request = { timeout = case.written } } }))

        local recorded = assert(fetches[1])
        t.assert_equals(recorded.options.timeout, case.seconds, tostring(case.written))
        t.assert_equals(recorded.client_opts.timeout, case.seconds, tostring(case.written))
    end
end

-- Негодная длительность — отказ с путём настройки и причиной ядра, а не
-- тихое умолчание.
g.test_bad_duration_is_rejected_with_its_path = function()
    local source = build_source(function()
        return 'groups: {}', nil, {}
    end)

    t.assert_error_msg_contains(
        '[tnt.ce.etcd] config.etcd.http.request.timeout: Unknown duration suffix "parsecs"',
        source.sync,
        source,
        nil,
        iconfig({ http = { request = { timeout = '3parsecs' } } })
    )

    t.assert_error_msg_contains(
        'config.etcd.watchers.reconnect_timeout: Expected duration as number or string',
        source.sync,
        source,
        nil,
        iconfig({ watchers = { reconnect_timeout = {} } })
    )
end

-- Настройки запроса из config.etcd.http.request доходят до клиента под
-- своими именами: сокет, интерфейс и подробный вывод.
g.test_request_options_are_passed_to_client = function()
    local source = build_source(function()
        return 'groups: {}', nil, {}
    end)

    source:sync(
        nil,
        iconfig({
            http = { request = { unix_socket = '/run/etcd.sock', interface = 'eth1', verbose = true } },
        })
    )

    local options = assert(fetches[1]).options
    t.assert_equals(options.unix_socket, '/run/etcd.sock')
    t.assert_equals(options.interface, 'eth1')
    t.assert_equals(options.verbose, true)
    t.assert_equals(options.timeout, 5, 'без срока — умолчание')
end

-- Раздел http без request — как отсутствующий: умолчания и ничего сверх.
g.test_http_section_without_request_yields_defaults = function()
    local source = build_source(function()
        return 'groups: {}', nil, {}
    end)

    source:sync(nil, iconfig({ http = {} }))

    local options = assert(fetches[1]).options
    t.assert_equals(options.timeout, 5)
    t.assert_equals(options.unix_socket, nil)
    t.assert_equals(options.verbose, nil)
end

-- После успешного чтения конфигурация сохраняется снимком.
g.test_successful_read_saves_snapshot = function()
    local source, path = build_source(function()
        return 'groups:\n  g: {}\n', nil, {}
    end)

    source:sync(nil, iconfig())

    t.assert_equals(fio.path.exists(path), true)
end

-- При недоступном etcd инстанс поднимается на снимке.
g.test_falls_back_to_snapshot = function()
    local available = true
    local source = build_source(function()
        if available then
            return 'groups:\n  from_etcd: {}\n', nil, {}
        end
        return nil, 'ни один адрес etcd не ответил', nil
    end)

    source:sync(nil, iconfig())
    available = false
    source:sync(nil, iconfig())

    t.assert_equals(source:get(), { groups = { from_etcd = {} } })
    t.assert_equals(source:status().stale, true, 'конфигурация помечена устаревшей')
    t.assert_str_contains(table.concat(warnings, '\n'), 'поднимается на снимке')
end

-- Без снимка стартовать не на чем: отказ, а не тихий подъём с пустой
-- конфигурацией.
g.test_without_snapshot_start_fails = function()
    local source = build_source(function()
        return nil, 'ни один адрес etcd не ответил', nil
    end)

    t.assert_error_msg_contains('стартовать не на чем', source.sync, source, nil, iconfig())
    t.assert_equals(#watchers, 0, 'отказ наблюдателя не заводит')
end

-- Выключенный откат означает отказ даже при наличии снимка.
g.test_disabled_fallback_fails_even_with_snapshot = function()
    local available = true
    local source = build_source(function()
        if available then
            return 'groups: {}', nil, {}
        end
        return nil, 'ни один адрес etcd не ответил', nil
    end)

    source:sync(nil, iconfig())
    available = false
    vars.TNT_CE_ETCD_FALLBACK = 'off'

    t.assert_error_msg_contains('откат на снимок выключен', source.sync, source, nil, iconfig())
end

-- Негодный выключатель отката останавливает чтение сразу, даже когда etcd
-- отвечает: иначе опечатка всплыла бы только в день его аварии.
g.test_unclear_fallback_fails_while_etcd_answers = function()
    local source = build_source(function()
        return 'groups: {}', nil, {}
    end)

    vars.TNT_CE_ETCD_FALLBACK = 'выкл'

    t.assert_error_msg_contains('TNT_CE_ETCD_FALLBACK', source.sync, source, nil, iconfig())
    t.assert_equals(#fetches, 0, 'в хранилище не ходили')
end

-- Без раздела etcd выключатель не читается: узлу без хранилища нет дела
-- до его отката.
g.test_fallback_is_not_read_without_etcd_section = function()
    local source = build_source(function() end)

    vars.TNT_CE_ETCD_FALLBACK = 'выкл'
    source:sync(nil, {})

    t.assert_equals(source:get(), {})
end

-- ── Каталог узла ─────────────────────────────────────────────────────

--- Хранилище, до которого не достучаться: узел поднимается на снимке.
---@return nil body
---@return string err
local function unreachable_storage()
    return nil, 'ни один адрес etcd не ответил'
end

--- Конфигурация инстанса с разделом etcd и рабочим каталогом.
---@param work_dir string
---@return table
local function iconfig_in(work_dir)
    local configured = iconfig()

    configured.process = { work_dir = work_dir }

    return configured
end

--- Снимок на диске с одной группой: по её имени видно, какой снимок взят.
---@param path string
---@param group string
local function planted_snapshot(path, group)
    fio.mktree(fio.dirname(path))

    local file = fio.open(path, { 'O_WRONLY', 'O_CREAT', 'O_TRUNC' }, tonumber('644', 8))

    file:write(('groups:\n  %s: {}\n'):format(group))
    file:close()
end

-- До `box.cfg` выключатель и снимок берутся из `.env` рабочего каталога,
-- а не каталога запуска: иначе старт и перечитывание после `box.cfg`
-- читали бы разные файлы. Выключатель каталога запуска здесь сказал бы
-- «нет», и подъём на снимке показывает, что его не читали.
g.test_snapshot_settings_come_from_the_work_dir = function()
    local work = fio.tempdir()
    local source = build_source(unreachable_storage, {
        files = {
            ['.env'] = 'TNT_CE_ETCD_FALLBACK=off\n',
            [work .. '/.env'] = 'TNT_CE_ETCD_SNAPSHOT=var/снимок.yaml\n',
        },
    })

    vars.TNT_CE_ETCD_SNAPSHOT = nil
    planted_snapshot(work .. '/var/снимок.yaml', 'from_work_dir')

    source:sync(nil, iconfig_in(work))

    t.assert_equals(source:get(), { groups = { from_work_dir = {} } })
    t.assert_equals(assert(warned[#warned].fields).snapshot, work .. '/var/снимок.yaml')
end

-- Имя инстанса для `{{ instance_name }}` источник спрашивает у ядра:
-- до `box.cfg` его больше знать некому.
g.test_instance_name_is_asked_of_the_kernel = function()
    local base = fio.tempdir()
    local work = base .. '/etcd-001-a'
    local source, path = build_source(unreachable_storage, {
        files = { [work .. '/.env'] = 'TNT_CE_ETCD_FALLBACK=off\n' },
    })

    -- Снимок каталога запуска на месте: не прочти источник `.env`
    -- рабочего каталога, узел поднялся бы на нём, а не отказал.
    planted_snapshot(path, 'from_launch_dir')

    t.assert_error_msg_contains(
        'откат на снимок выключен',
        source.sync,
        source,
        { _instance_name = 'etcd-001-a' },
        iconfig_in(base .. '/{{ instance_name }}')
    )
end

-- Каталог, который до `box.cfg` не раскрыть, узла не роняет: пути
-- считаются от текущего каталога, а журнал называет причину — иначе
-- оператор не узнал бы, что его `.env` в рабочем каталоге не прочитан.
g.test_unresolvable_work_dir_is_reported_and_the_current_directory_used = function()
    local source, path = build_source(unreachable_storage)
    local work_dir = 'var/{{ replicaset_name }}'

    planted_snapshot(path, 'from_launch_dir')

    source:sync({ _instance_name = 'etcd-001-a' }, iconfig_in(work_dir))

    t.assert_equals(source:get(), { groups = { from_launch_dir = {} } })
    t.assert_equals(warned[1], {
        message = 'каталог узла не определён: .env и снимок считаются от текущего',
        fields = {
            err = ('process.work_dir «%s»: до box.cfg раскрывается только {{ instance_name }}'):format(
                work_dir
            ),
        },
    })
end

-- `.env` читается однажды на одно чтение конфигурации: выключатель,
-- поиск снимка, время его правки и запись в журнал берут одно прочтение.
-- Следующее чтение конфигурации читает файл заново.
g.test_env_file_is_read_once_per_sync = function()
    local source, path = build_source(unreachable_storage)
    local reads = helper.counted_environment(vars, { ['.env'] = 'TNT_CE_ETCD_FALLBACK=on\n' })

    planted_snapshot(path, 'g')

    source:sync(nil, iconfig())
    t.assert_equals(reads, { '.env' })

    source:sync(nil, iconfig())
    t.assert_equals(reads, { '.env', '.env' })
end

-- Повреждённый снимок не выдаётся за рабочую конфигурацию.
g.test_broken_snapshot_is_rejected = function()
    local source, path = build_source(function()
        return nil, 'ни один адрес etcd не ответил', nil
    end)

    local file = fio.open(path, { 'O_WRONLY', 'O_CREAT' }, tonumber('644', 8))
    file:write('это: [не: закрыто')
    file:close()

    t.assert_error_msg_contains('снимок повреждён', source.sync, source, nil, iconfig())
end

-- Несохранившийся снимок не мешает работе: он подстраховка, а не источник правды.
g.test_snapshot_write_failure_only_warns = function()
    helper.snapshot({ TNT_CE_ETCD_SNAPSHOT = '/каталога/нет/snapshot.yaml' })
    local source_module = testing.load_sources(SOURCE, 'tnt.ce.etcd.source')
    local logged = {}

    local source = source_module.new({
        client_factory = function()
            return {
                fetch = function()
                    return 'groups: {}', nil, {}
                end,
            }
        end,
        logger = {
            warn = function(format, ...)
                table.insert(logged, string.format(format, ...))
            end,
            info = function() end,
        },
    })

    source:sync(nil, iconfig())

    t.assert_equals(source:get(), { groups = {} })
    t.assert_str_contains(table.concat(logged, '\n'), 'снимок конфигурации не сохранён')
end

-- Смешанный случай: канонического ключа нет, а устаревший недоступен
-- по связи. Это отказ, а не «конфигурации нет».
g.test_missing_primary_with_transport_failure_falls_back = function()
    local source, path = build_source(function(key)
        if key == '/tnt/config/all' then
            return nil, 'ключ не найден', { missing = true }
        end
        return nil, 'ни один адрес etcd не ответил', nil
    end)

    local file = fio.open(path, { 'O_WRONLY', 'O_CREAT' }, tonumber('644', 8))
    file:write('groups:\n  from_snapshot: {}\n')
    file:close()

    source:sync(nil, iconfig())

    t.assert_equals(
        source:status().stale,
        true,
        'подъём на снимке, а не пустая конфигурация'
    )
end

-- Конфигурация, разобравшаяся не в таблицу, отвергается.
g.test_non_table_yaml_is_rejected = function()
    local source = build_source(function()
        return 'просто строка', nil, {}
    end)

    t.assert_error_msg_contains('не разобрана как YAML', source.sync, source, nil, iconfig())
end

-- До первого чтения конфигурация не считается устаревшей.
g.test_fresh_source_is_not_stale = function()
    local source = build_source(function() end)

    t.assert_equals(source:status().stale, false)
end

-- Без таймаута в конфигурации клиент получает умолчание.
g.test_default_timeout_is_used = function()
    local source = build_source(function()
        return 'groups: {}', nil, {}
    end)

    source:sync(nil, iconfig())

    local recorded = assert(fetches[1])
    t.assert_equals(recorded.options.timeout, 5)
    t.assert_equals(recorded.client_opts.timeout, 5)
end

-- Без подменённой фабрики источник строит настоящего клиента etcd.
g.test_default_client_factory_builds_client = function()
    local source_module = testing.load_sources(SOURCE, 'tnt.ce.etcd.source')
    local source = source_module.new({})

    -- Клиент создаётся, но в сеть не ходит: проверяется только его контракт.
    local client = source._client_factory({ timeout = 3 })

    t.assert_type(client, 'table')
    t.assert_type(client.fetch, 'function')
    t.assert_type(client.request, 'function')
end

-- Состояние отражает готовность источника.
g.test_status_reports_readiness = function()
    local source = build_source(function()
        return 'groups: {}', nil, { mod_revision = 3, endpoint = 'http://etcd:2379' }
    end)

    t.assert_equals(source:status().ready, false)

    source:sync(nil, iconfig())

    local status = source:status()
    t.assert_equals(status.ready, true)
    t.assert_equals(status.endpoint, 'http://etcd:2379')
    t.assert_type(status.synced_at, 'number')
end

-- ── Наблюдение за ключом ─────────────────────────────────────────────

--- Ответ etcd с конфигурацией и ревизиями.
---
--- Ревизия хранилища по умолчанию обгоняет ревизию ключа: в etcd пишут
--- не только конфигурацию, и совпадение их было бы случайностью.
---@param revision number Ревизия ключа
---@param store_revision number|nil Ревизия хранилища
---@return fun(key: string): string|nil, string|nil, table|nil
local function answers_with(revision, store_revision)
    return function(key)
        if key == '/tnt/config/all' then
            return 'groups: {}',
                nil,
                {
                    endpoint = 'http://etcd:2379',
                    revision = store_revision or revision + 100,
                    mod_revision = revision,
                }
        end

        return nil, 'ключ не найден', { missing = true }
    end
end

--- Делает один такт наблюдателя.
local function tick()
    assert(watchers[1], 'наблюдатель не запускался')()
end

g.test_watching_starts_with_the_first_read = function()
    -- Пока конфигурация не прочитана, следить не за чем: адреса и ключи
    -- источник узнаёт из неё же.
    local source = build_source(answers_with(7))

    t.assert_equals(source:status().watching, false)

    source:sync(nil, iconfig())

    t.assert_equals(source:status().watching, true)
    t.assert_equals(#watchers, 1)
end

g.test_repeated_sync_does_not_add_a_second_watcher = function()
    -- Ядро зовёт sync при каждом перечитывании конфигурации, и каждый
    -- раз заводить наблюдателя значило бы плодить их до бесконечности.
    local source = build_source(answers_with(7))

    source:sync(nil, iconfig())
    source:sync(nil, iconfig())

    t.assert_equals(#watchers, 1)
end

g.test_new_revision_makes_the_node_reread_the_configuration = function()
    -- Узел, не заметивший правку, остаётся на прежней конфигурации
    -- навсегда: запись прошла, а он о ней не знает.
    local revision = 7
    local source = build_source(function(key)
        return answers_with(revision)(key)
    end)

    source:sync(nil, iconfig())
    revision = 8

    tick()

    t.assert_equals(reloads, 1)
end

g.test_same_revision_changes_nothing = function()
    local source = build_source(answers_with(7))

    source:sync(nil, iconfig())
    tick()

    t.assert_equals(reloads, 0)
end

g.test_foreign_write_to_the_storage_changes_nothing = function()
    -- Назначение лидера, замок, чужая правка рядом двигают ревизию всего
    -- хранилища, а конфигурацию не трогают. Перечитывать её на каждую
    -- такую запись значило бы переприменять весь кластер ровно в миг
    -- смены лидера.
    local store_revision = 50
    local source = build_source(function(key)
        return answers_with(7, store_revision)(key)
    end)

    source:sync(nil, iconfig())
    store_revision = 60

    tick()

    t.assert_equals(reloads, 0)
end

g.test_silent_storage_is_not_a_change = function()
    -- Молчащее хранилище не повод перечитывать конфигурацию: узел
    -- продолжает жить на том, что у него есть.
    local alive = true
    local source = build_source(function(key)
        if not alive then
            return nil, 'нет связи', nil
        end

        return answers_with(7)(key)
    end)

    source:sync(nil, iconfig())
    alive = false

    tick()

    t.assert_equals(reloads, 0)
    t.assert_equals(source:status().watching, false, 'наблюдатель кончил такт сам')

    -- И это не срыв наблюдения: хранилище просто молчит, а узел живёт
    -- на том, что у него есть.
    for _, warning in ipairs(warnings) do
        t.assert_not_str_contains(warning, 'сорвалось')
    end
end

g.test_failed_reread_does_not_kill_the_watcher = function()
    -- Перечитывание валится, например, когда узел вычеркнут из новой
    -- конфигурации. Следующая правка может его вернуть, и наблюдать
    -- за ней должно быть кому.
    local revision = 7
    local source = build_source(function(key)
        return answers_with(revision)(key)
    end)

    source:sync(nil, iconfig())
    revision = 8
    reload_error = 'узла в конфигурации нет'

    tick()

    t.assert_equals(reloads, 1)
    t.assert_str_contains(warnings[#warnings], 'конфигурация не перечитана')
end

g.test_broken_watch_request_is_reported = function()
    -- Обращение к хранилищу срывается и исключением: наблюдатель обязан
    -- пережить это и сказать вслух.
    local source = build_source(answers_with(7))

    source:sync(nil, iconfig())

    source._watch_request.client.fetch = function()
        error('клиент сломался')
    end

    tick()

    t.assert_str_contains(
        warnings[#warnings],
        'наблюдение за конфигурацией сорвалось'
    )
end

g.test_watching_can_be_stopped = function()
    local source = build_source(answers_with(7))

    source:sync(nil, iconfig())
    source:unwatch()

    t.assert_equals(source:status().watching, false)

    tick()

    t.assert_equals(reloads, 0, 'остановленный наблюдатель ничего не делает')
end

g.test_watching_can_be_switched_off = function()
    -- Кластеру, где конфигурацию правят только с перезапуском, наблюдение
    -- не нужно: лишний запрос в хранилище каждые несколько секунд.
    local source = build_source(answers_with(7), { watch_interval = false })

    source:sync(nil, iconfig())

    t.assert_equals(source:status().watching, false)
    t.assert_equals(#watchers, 0)
end

g.test_configuration_read_from_the_legacy_key_is_watched_too = function()
    -- Старый ключ читается так же, как основной, и следить за ним надо
    -- ровно так же.
    local source = build_source(function(key)
        if key == '/tnt/config' then
            return 'groups: {}', nil, { endpoint = 'http://etcd:2379', mod_revision = 5 }
        end

        return nil, 'ключ не найден', { missing = true }
    end)

    source:sync(nil, iconfig())

    t.assert_equals(source:status().watching, true)
end

g.test_watcher_started_before_any_read_asks_nobody = function()
    -- Наблюдение можно включить и до первого чтения: адресов оно ещё
    -- не знает, и спрашивать ему некого — но падать от этого нельзя.
    local source = build_source(answers_with(7))

    source:watch()
    tick()

    t.assert_equals(reloads, 0)
    t.assert_equals(#fetches, 0)
end

g.test_configuration_is_reread_by_the_instance_itself = function()
    -- Перечитывание берётся у самого узла: подмена в остальных проверках
    -- скрывает как раз этот вызов, а он и есть всё действие наблюдателя.
    local asked = false

    package.loaded['config'] = {
        reload = function()
            asked = true
        end,
    }

    local revision = 7

    -- Снимок — из исходника: установленная копия могла бы отстать от
    -- источника, а записанный ею файл лёг бы в `var/` дерева проекта.
    helper.snapshot({ TNT_CE_ETCD_SNAPSHOT = fio.pathjoin(fio.tempdir(), 'snapshot.yaml') })

    local source_module = testing.load_sources(SOURCE, 'tnt.ce.etcd.source')
    local source = source_module.new({
        client_factory = function()
            return {
                fetch = function(_, _, key)
                    if key == '/tnt/config/all' then
                        return 'groups: {}', nil, { endpoint = 'http://etcd:2379', mod_revision = revision }
                    end

                    return nil, 'ключ не найден', { missing = true }
                end,
            }
        end,
        logger = { warn = function() end, info = function() end },
        spawn = function() end,
        sleep = function() end,
    })

    source:sync(nil, iconfig())
    revision = 8

    -- Тело наблюдателя не запускалось: проверяется ровно один его шаг —
    -- тот, которым он просит узел перечитать конфигурацию.
    source._reload()

    package.loaded['config'] = nil

    t.assert_equals(asked, true)
end

--- Наблюдение с подменённым ожиданием: первый заход останавливает цикл,
--- а срок ожидания остаётся видимым проверке.
---@param interval any Значение watch_interval
---@return table source
---@return fun(): number|nil waited
local function watching_with(interval)
    ---@type any
    local source
    local waited

    source = build_source(answers_with(7), {
        watch_interval = interval,
        sleep = function(seconds)
            waited = seconds
            source:unwatch()
        end,
    })

    return source, function()
        return waited
    end
end

g.test_watch_interval_defaults_to_five_seconds = function()
    -- Столько же, сколько по умолчанию ждёт наблюдение в самом Tarantool.
    -- Чаще незачем: правку делают руками, а запрос идёт по сети и стоит.
    local source, waited = watching_with(nil)

    source:sync(nil, iconfig())
    tick()

    t.assert_equals(waited(), 5)
end

g.test_given_watch_interval_replaces_the_default = function()
    local source, waited = watching_with(0.25)

    source:sync(nil, iconfig())
    tick()

    t.assert_equals(waited(), 0.25)
end

g.test_configuration_read_without_a_key_revision_is_not_reread = function()
    -- Ревизии ключа у прочитанной конфигурации нет — сравнивать не с чем.
    -- Перечитывание принесло бы ту же безымянную конфигурацию и
    -- повторялось бы каждый такт.
    ---@type number|nil
    local revision = nil
    local source = build_source(function()
        return 'groups: {}', nil, { endpoint = 'http://etcd:2379', mod_revision = revision }
    end)

    source:sync(nil, iconfig())
    revision = 8

    tick()

    t.assert_equals(reloads, 0)
    t.assert_equals(warnings, {}, 'и такт не сорвался')
end

g.test_answer_without_a_key_revision_changes_nothing = function()
    -- Хранилище отдало ключ без ревизии: с прочитанной её не сравнить,
    -- и это не повод ни перечитывать, ни срываться.
    local answered = false
    local source = build_source(function()
        if answered then
            return 'groups: {}', nil, { endpoint = 'http://etcd:2379' }
        end

        return 'groups: {}', nil, { endpoint = 'http://etcd:2379', mod_revision = 7 }
    end)

    source:sync(nil, iconfig())
    answered = true

    tick()

    t.assert_equals(reloads, 0)
    t.assert_equals(warnings, {}, 'и такт не сорвался')
end

-- ── Настройки наблюдения из config.etcd.watchers ─────────────────────

--- Источник, чей наблюдатель делает ровно `rounds` заходов; каждое
--- ожидание записывается, и по ним видно, какой срок выбран.
---@param rounds integer
---@param responder fun(key: string): string|nil, string|nil, table|nil
---@return table source
---@return number[] waits
local function watching_rounds(rounds, responder)
    ---@type any
    local source
    local waits = {}

    source = build_source(responder, {
        sleep = function(seconds)
            table.insert(waits, seconds)

            if #waits > rounds then
                source:unwatch()
            end
        end,
    })

    return source, waits
end

--- Хранилище, которое молчит, пока не сказано иное.
---@return fun(key: string): string|nil, string|nil, table|nil responder
---@return fun(alive: boolean) set_alive
local function silent_storage()
    local alive = true

    return function(key)
        if not alive then
            return nil, 'нет связи', nil
        end

        return answers_with(7)(key)
    end, function(value)
        alive = value
    end
end

g.test_reconnect_timeout_is_waited_after_a_silent_attempt = function()
    -- После молчания хранилища ждут не обычный такт, а срок переподключения
    -- из конфигурации — записанный так же, как его принимает ядро.
    local responder, set_alive = silent_storage()
    local source, waits = watching_rounds(2, responder)

    source:sync(nil, iconfig({ watchers = { reconnect_timeout = '2s' } }))
    set_alive(false)
    tick()

    t.assert_equals(
        waits,
        { 5, 2, 2 },
        'первый такт обычный, после молчания — срок переподключения'
    )
end

g.test_without_reconnect_timeout_the_usual_interval_is_kept = function()
    local responder, set_alive = silent_storage()
    local source, waits = watching_rounds(1, responder)

    source:sync(nil, iconfig())
    set_alive(false)
    tick()

    t.assert_equals(waits, { 5, 5 })
end

g.test_answer_after_silence_returns_to_the_usual_interval = function()
    -- Ответившее хранилище закрывает полосу молчания: следующий такт
    -- снова обычный, а счёт неудач начинается с нуля, а не продолжается.
    local responder, set_alive = silent_storage()
    local source, waits = watching_rounds(3, responder)

    source:sync(nil, iconfig({ watchers = { reconnect_timeout = 2, reconnect_max_attempts = 2 } }))
    -- Ожидание — единственное место, откуда проверка может вмешаться
    -- между заходами: первый заход застаёт хранилище молчащим, второй —
    -- ожившим, третий — снова молчащим.
    local raw_sleep = source._sleep
    source._sleep = function(seconds)
        raw_sleep(seconds)
        set_alive(#waits == 2)
    end
    tick()

    t.assert_equals(waits, { 5, 2, 5, 2 })
    t.assert_equals(source:status().watching, false, 'наблюдатель кончил заходы сам')
    t.assert_equals(errors, {}, 'две неудачи не подряд — не предел')
end

g.test_watching_stops_after_the_limit_of_silent_attempts = function()
    -- Столько молчаний подряд, сколько разрешено, — и наблюдение
    -- останавливается вслух, а не крутится впустую.
    local responder, set_alive = silent_storage()
    local source = watching_rounds(5, responder)

    source:sync(nil, iconfig({ watchers = { reconnect_max_attempts = 2 } }))
    set_alive(false)
    tick()

    t.assert_equals(source:status().watching, false)
    t.assert_equals(#fetches, 3, 'чтение и ровно две попытки')
    t.assert_equals(errors, {
        {
            message = 'наблюдение за конфигурацией остановлено: хранилище не отвечает',
            fields = { attempts = 2, err = 'нет связи' },
        },
    })
end

g.test_one_silence_short_of_the_limit_keeps_watching = function()
    local responder, set_alive = silent_storage()
    local source = watching_rounds(1, responder)

    source:sync(nil, iconfig({ watchers = { reconnect_max_attempts = 2 } }))
    set_alive(false)
    tick()

    t.assert_equals(errors, {})
    t.assert_equals(#fetches, 2, 'одна попытка — ещё не предел')
end

g.test_zero_attempts_stop_at_the_first_silence = function()
    -- Ноль попыток переподключения — остановка на первом же молчании.
    local responder, set_alive = silent_storage()
    local source = watching_rounds(5, responder)

    source:sync(nil, iconfig({ watchers = { reconnect_max_attempts = 0 } }))
    set_alive(false)
    tick()

    t.assert_equals(source:status().watching, false)
    t.assert_equals(#fetches, 2)
    t.assert_equals(assert(errors[1]).fields, { attempts = 1, err = 'нет связи' })
end

g.test_without_a_limit_silence_is_tolerated_indefinitely = function()
    local responder, set_alive = silent_storage()
    local source = watching_rounds(10, responder)

    source:sync(nil, iconfig())
    set_alive(false)
    tick()

    t.assert_equals(errors, {})
    t.assert_equals(#fetches, 11, 'чтение и десять попыток')
end

g.test_a_broken_attempt_counts_as_a_failure = function()
    -- Сорвавшееся исключением обращение — тоже неудачная попытка:
    -- для оператора хранилище и в этом случае не ответило.
    local source, waits = watching_rounds(5, answers_with(7))

    source:sync(nil, iconfig({ watchers = { reconnect_max_attempts = 1 } }))
    source._watch_request.client.fetch = function()
        error('клиент сломался', 0)
    end
    tick()

    t.assert_equals(source:status().watching, false)
    t.assert_equals(waits, { 5 }, 'остановился после первой же попытки')
    t.assert_str_contains(warnings[#warnings], 'сорвалось')
    t.assert_equals(assert(errors[1]).fields, { attempts = 1, err = 'клиент сломался' })
end

g.test_a_missing_key_is_an_answer_not_a_silence = function()
    -- Хранилище ответило, что ключа нет: это не молчание, и попытки
    -- переподключения не тратятся.
    local source = watching_rounds(3, function()
        return nil, 'ключ не найден', { missing = true }
    end)

    source:sync(nil, iconfig({ watchers = { reconnect_max_attempts = 1 } }))
    tick()

    t.assert_equals(source:status().watching, false, 'наблюдатель кончил заходы сам')
    t.assert_equals(#fetches, 8, 'чтение и три захода — по два ключа')
    t.assert_equals(errors, {})
    t.assert_equals(reloads, 0)
end

g.test_watcher_before_any_read_does_not_spend_attempts = function()
    -- До первого чтения спрашивать некого, и это не неудача.
    local source = watching_rounds(3, answers_with(7))

    source._watchers = { reconnect_max_attempts = 1 }
    source:watch()
    tick()

    t.assert_equals(errors, {})
    t.assert_equals(warnings, {})
end

g.test_stopped_watching_is_restarted_by_the_next_sync = function()
    -- Остановленное наблюдение заводит заново перечитывание конфигурации:
    -- оператор, вернув хранилище, зовёт reload — и узел снова следит.
    local responder, set_alive = silent_storage()
    local source = watching_rounds(5, responder)

    source:sync(nil, iconfig({ watchers = { reconnect_max_attempts = 1 } }))
    set_alive(false)
    tick()
    t.assert_equals(source:status().watching, false)

    set_alive(true)
    source:sync(nil, iconfig({ watchers = { reconnect_max_attempts = 1 } }))

    t.assert_equals(source:status().watching, true)
    t.assert_equals(#watchers, 2)
end

-- ── Снимок, пустой ключ и старый ключ ────────────────────────────────

--- Хранилище, которое проверка меняет по ходу: молчит либо держит
--- ключи с ревизиями.
---@return fun(key: string): string|nil, string|nil, table|nil responder
---@return { silent: boolean, keys: table<string, number> } state Ключ — его ревизия
local function storage()
    ---@type { silent: boolean, keys: table<string, number> }
    local state = { silent = false, keys = {} }

    return function(key)
        if state.silent then
            return nil, 'нет связи', nil
        end

        local revision = state.keys[key]

        if revision == nil then
            return nil, 'ключ не найден', { missing = true }
        end

        return 'groups: {}', nil, { endpoint = 'http://etcd:2379', mod_revision = revision }
    end,
        state
end

--- Перечитывание, как его делает ядро: снова зовёт sync.
---
--- Так видно, что узел не просто попросил перечитать, а сошёл со снимка,
--- и что следующий такт не просит того же снова.
---@param source table
local function rereads_like_the_kernel(source)
    source._reload = function()
        reloads = reloads + 1
        source:sync(nil, iconfig())
    end
end

--- Ключи, которые спрашивали после отметки.
---@param from integer Сколько обращений было до отметки
---@return string[]
local function keys_asked_since(from)
    local keys = {}

    for index, recorded in ipairs(fetches) do
        if index > from then
            table.insert(keys, recorded.key)
        end
    end

    return keys
end

g.test_node_started_on_a_snapshot_rereads_once_etcd_answers = function()
    -- Узел, поднятый при лежащем etcd, живёт на снимке. Вернувшийся etcd
    -- он обязан заметить сам: ручного перечитывания может не случиться
    -- никогда, а снимок стареет с каждой правкой.
    local responder, state = storage()
    local source = watching_rounds(2, responder)

    rereads_like_the_kernel(source)
    planted_snapshot(vars.TNT_CE_ETCD_SNAPSHOT, 'from_snapshot')
    state.silent = true

    source:sync(nil, iconfig())

    t.assert_equals(source:status().stale, true)
    t.assert_equals(source:status().watching, true, 'на снимке наблюдение идёт')

    state.silent = false
    state.keys['/tnt/config/all'] = 7
    tick()

    -- Второй такт видит ту же ревизию и не просит снова.
    t.assert_equals(reloads, 1)
    t.assert_equals(source:status().stale, false)
    t.assert_equals(source:status().revision, 7)
    t.assert_equals(source:get(), { groups = {} })
end

g.test_node_started_without_a_key_rereads_once_it_is_written = function()
    -- Узел, поднятый до первой записи конфигурации, получает её сам,
    -- а не на ближайшем ручном перечитывании.
    local responder, state = storage()
    local source = watching_rounds(2, responder)

    rereads_like_the_kernel(source)

    source:sync(nil, iconfig())

    t.assert_equals(source:status().watching, true, 'без ключа наблюдение идёт')

    state.keys['/tnt/config/all'] = 3
    tick()

    t.assert_equals(reloads, 1)
    t.assert_equals(source:status().revision, 3)
    t.assert_equals(source:get(), { groups = {} })
end

g.test_edit_of_the_legacy_key_is_noticed = function()
    -- Развёртывание на старом ключе правит старый ключ: наблюдение,
    -- спрашивающее только основной, этой правки не видело вовсе.
    local responder, state = storage()
    local source = watching_rounds(1, responder)

    state.keys['/tnt/config'] = 5
    source:sync(nil, iconfig())
    state.keys['/tnt/config'] = 6

    local before = #fetches
    tick()

    t.assert_equals(reloads, 1)
    t.assert_equals(
        keys_asked_since(before),
        { '/tnt/config/all', '/tnt/config' },
        'ключи — в том же порядке, в каком их читает sync'
    )
end

g.test_unchanged_legacy_key_is_not_reread = function()
    -- Конфигурация на старом ключе — та же, что прочитана: перечитывать
    -- её каждый такт значило бы переприменять узел без конца.
    local responder, state = storage()
    local source = watching_rounds(1, responder)

    state.keys['/tnt/config'] = 5
    source:sync(nil, iconfig())

    tick()

    t.assert_equals(reloads, 0)
end

g.test_primary_key_written_over_the_legacy_one_is_a_change = function()
    -- Переезд на основной ключ: sync теперь прочитал бы его, и узлу пора
    -- перечитать. Решает ключ, а не число: у разных ключей ревизии между
    -- собой не сравнивают.
    local responder, state = storage()
    local source = watching_rounds(1, responder)

    state.keys['/tnt/config'] = 5
    source:sync(nil, iconfig())
    state.keys['/tnt/config/all'] = 2

    tick()

    t.assert_equals(reloads, 1)
end

g.test_deleted_key_is_not_a_change_and_not_a_silence = function()
    -- Пропавший ключ перечитывание сменило бы пустой конфигурацией вместо
    -- рабочей. Узел живёт на той, что есть, а попытки не тратятся:
    -- хранилище ответило.
    local responder, state = storage()
    local source = watching_rounds(2, responder)

    state.keys['/tnt/config/all'] = 7
    source:sync(nil, iconfig({ watchers = { reconnect_max_attempts = 1 } }))
    state.keys = {}

    tick()

    t.assert_equals(reloads, 0)
    t.assert_equals(errors, {})
end

g.test_snapshot_taken_after_a_read_is_left_at_the_same_revision = function()
    -- Узел прочитал ревизию 7, перечитывание застало etcd лежащим, и узел
    -- ушёл на снимок. Вернувшийся etcd с той же ревизией — всё равно
    -- повод перечитать: узел помечен устаревшим, пока не прочтёт заново.
    local responder, state = storage()
    local source = watching_rounds(1, responder)

    rereads_like_the_kernel(source)
    state.keys['/tnt/config/all'] = 7
    source:sync(nil, iconfig())
    state.silent = true
    source:sync(nil, iconfig())

    t.assert_equals(source:status().stale, true)

    state.silent = false
    tick()

    t.assert_equals(reloads, 1)
    t.assert_equals(source:status().stale, false)
end

g.test_key_written_again_after_it_was_emptied_is_a_change = function()
    -- Ключ стёрли, перечитывание дало пустую конфигурацию, ключ записали
    -- снова: узел обязан его прочитать, какой бы ни была ревизия.
    local responder, state = storage()
    local source = watching_rounds(1, responder)

    state.keys['/tnt/config/all'] = 7
    source:sync(nil, iconfig())
    state.keys = {}
    source:sync(nil, iconfig())
    state.keys['/tnt/config/all'] = 7

    tick()

    t.assert_equals(reloads, 1)
end

-- ── Опознаватель захода ──────────────────────────────────────────────

g.test_every_watch_round_runs_under_a_request_id_of_its_own = function()
    -- Записи захода и перечитывание, которое он попросил, связаны одним
    -- опознавателем, соседние заходы — разными, а в паузе между заходами
    -- опознавателя нет: область живёт ровно заход.
    local revision = 7
    local paused = {}

    ---@type any
    local source

    source = build_source(function(key)
        return answers_with(revision)(key)
    end, {
        sleep = function()
            local context = testing.module('tnt.context')

            table.insert(paused, context.get(context.REQUEST_ID) or false)

            if #paused > 2 then
                source:unwatch()
            end
        end,
    })

    source:sync(nil, iconfig())
    revision = 8
    reload_error = 'узла в конфигурации нет'
    marks = {}

    tick()

    local first = assert(marks[1], 'первого захода не было').request_id
    local second = assert(marks[3], 'второго захода не было').request_id

    t.assert_equals(paused, { false, false, false })
    t.assert_equals(marks, {
        { what = 'reload', request_id = first },
        { what = 'warn', request_id = first },
        { what = 'reload', request_id = second },
        { what = 'warn', request_id = second },
    })
    t.assert_equals(testing.module('tnt.id').is_ulid(first), true)
    t.assert_not_equals(second, first)
end

g.test_stopped_watching_is_reported_under_its_last_round = function()
    -- Отказ, остановивший наблюдение, — происшествие последнего захода:
    -- по его опознавателю находится и срыв, который к нему привёл.
    local source = watching_rounds(5, answers_with(7))

    source:sync(nil, iconfig({ watchers = { reconnect_max_attempts = 1 } }))
    source._watch_request.client.fetch = function()
        error('клиент сломался')
    end
    marks = {}

    tick()

    local round = assert(marks[1], 'захода не было').request_id

    t.assert_equals(marks, {
        { what = 'warn', request_id = round },
        { what = 'error', request_id = round },
    })
    t.assert_equals(testing.module('tnt.id').is_ulid(round), true)
end
