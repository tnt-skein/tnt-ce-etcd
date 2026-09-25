--- Источник конфигурации кластера из etcd для Community Edition.
---
--- В Enterprise такой источник встроен в модуль internal.config.extras.
--- Здесь он написан заново поверх HTTP-шлюза etcd API v3 и подключается
--- через каркас tnt-ce-extras.
---
--- Контракт источника задан ядром (см. _register_source в
--- src/box/lua/config/init.lua): обязательны поля name и type, а также
--- методы sync и get.
---
--- Права. `sync` ядро зовёт в файбере того, кто читает конфигурацию, и от
--- его учётки, без `box.session.su`: при старте это `admin`, при
--- перечитывании — вызывающий, например узкая учётка оператора с правом
--- на одну функцию перечитывания. Поэтому источник обходится без
--- спейсов `box`: узкой учётке они закрыты, и чтение из них уронило бы
--- перечитывание оператора. Наблюдатель, рождённый таким вызовом
--- (наблюдение, остановленное молчанием хранилища, заводит заново
--- перечитывание), живёт от `admin`, и это задумано: наблюдение — работа
--- узла, а не оператора, и обязано пережить вызов. Проверено на живом
--- ядре 3.8.
---
--- Наблюдение за ключом — в `tnt.ce.etcd.watch`: оно работает над
--- состоянием источника, а отдельным файлом потому, что чтение
--- конфигурации и наблюдение за ней — две работы.
---
--- Откат на локальный снимок: при недоступном etcd инстанс поднимается
--- на последней успешно прочитанной конфигурации вместо того, чтобы
--- не подняться вовсе, — иначе авария хранилища конфигурации не пускала
--- бы кластер перезапуститься.
---
--- Раздел config.etcd читается ровно по схеме ядра — endpoints, prefix,
--- username, password, ssl.*, http.request.*, watchers.* — и под теми же
--- именами: схема с пакетом tnt-ce-extras открыта, но не отключена, и
--- ключ вне её ядро отвергает («Unexpected field»). Поэтому конфигурация,
--- написанная для этого источника, переезжает на Enterprise как есть, а
--- длительности в ней записываются так же, как принимает ядро: числом
--- секунд либо строкой с единицей (`30s`, `5m`).

local yaml = require('yaml')
local clock = require('clock')
local fiber = require('fiber')
-- Разбор длительностей — ядра: свой разошёлся бы с ним на первой же
-- новой единице. Модуль внутренний, как и вся дверь для сторонних
-- источников, и в аннотациях Tarantool его нет; в 3.8 он есть.
---@diagnostic disable-next-line: unresolved-require
local units = require('internal.config.utils.units')

local snapshot = require('tnt.ce.etcd.snapshot')
local watch = require('tnt.ce.etcd.watch')

--- Отказ источника — текстом без места в коде: его читает оператор
--- в выводе старта узла, и приписка «source.lua:NN:» там только мешает.
local raise = require('tnt.must.fail').raise

---@class TntEtcdSourceOptions
---@field name string|nil Имя источника; по умолчанию etcd
---@field client_factory (fun(opts: table): TntCeEtcdClient)|nil Фабрика клиента
---@field logger TntLogger|nil Журнал; по умолчанию tnt.log под именем tnt.ce.etcd
---@field watch_interval number|nil Как часто смотреть, не изменилась ли конфигурация
---@field reload (fun())|nil Чем перечитывать конфигурацию; по умолчанию config:reload()
---@field spawn (fun(body: fun()))|nil Чем запускать наблюдателя; по умолчанию фибер
---@field sleep (fun(seconds: number))|nil Чем ждать между проверками

---@class TntEtcdSource
---@field name string Имя источника для ядра
---@field type string Тип источника: cluster
---@field _client_factory fun(opts: table): TntCeEtcdClient
---@field _log TntLogger Журнал
---@field _payload table|nil Прочитанная конфигурация
---@field _key string|nil Ключ, с которого прочитана конфигурация; nil — она со снимка или ключа нет
---@field _revision number|nil Ревизия ключа, с которого прочитана конфигурация
---@field _endpoint string|nil Адрес, с которого прочитали
---@field _synced_at number|nil Время последнего чтения
---@field _stale boolean Конфигурация взята из снимка
---@field _watching boolean Идёт ли наблюдение за ключом; снимается извне
---@field _watch_enabled boolean Заведено ли наблюдение вообще
---@field _watch_interval number Как часто смотреть, не изменилась ли конфигурация
---@field _watch_request TntEtcdRequest|nil Чем и куда ходить наблюдению
---@field _watchers TntEtcdWatchers Настройки наблюдения из config.etcd.watchers
---@field _reload fun() Чем перечитывать конфигурацию
---@field _spawn fun(body: fun()) Чем запускать наблюдателя
---@field _sleep fun(seconds: number) Чем ждать между проверками
local Module = {}
Module.__index = Module

--- Таймаут запроса к etcd, если не задан в конфигурации.
local DEFAULT_TIMEOUT = 5

--- Как часто смотреть, не изменилась ли конфигурация, если не задано иное.
---
--- Пять секунд — столько же, сколько по умолчанию ждёт наблюдение в самом
--- Tarantool. Чаще незачем: правку конфигурации делают руками, и секунда
--- разницы никого не спасёт, а запрос идёт по сети и стоит.
local DEFAULT_WATCH_INTERVAL = 5

--- Создаёт источник.
---@param opts TntEtcdSourceOptions|nil
---@return TntEtcdSource
function Module.new(opts)
    opts = opts or {}

    return setmetatable({
        -- Поля контракта: ядро читает их напрямую, прятать под подчёркивание
        -- нельзя.
        name = opts.name or 'etcd',
        type = 'cluster',

        -- Фабрика клиента вынесена в параметр ради проверяемости: тест
        -- подставляет двойник вместо настоящих запросов в сеть.
        _client_factory = opts.client_factory or function(client_opts)
            return require('tnt.ce.etcd.client').new(client_opts)
        end,

        -- Журнал фасада, а не встроенный: в причине отказа etcd лежит адрес
        -- хранилища, а в адресе бывает пароль, и встроенный журнал напечатал
        -- бы его как есть. Берётся при сборке, а не при загрузке модуля:
        -- источник заводится до box.cfg, и лишнего в этот миг грузить незачем.
        _log = opts.logger or require('tnt.log').new('tnt.ce.etcd'),
        _payload = nil,
        _key = nil,
        _revision = nil,
        _endpoint = nil,
        _synced_at = nil,
        _stale = false,

        -- Наблюдение за ключом. В Enterprise встроенный источник замечает
        -- новую конфигурацию сам; здесь это делает наблюдатель, потому что
        -- узел, не заметивший правку, остаётся на прежней навсегда. Почему
        -- опросом, а не потоком, — в шапке `tnt.ce.etcd.watch`.
        _watching = false,
        _watch_interval = tonumber(opts.watch_interval) or DEFAULT_WATCH_INTERVAL,
        _watch_enabled = opts.watch_interval ~= false,
        _watchers = {},

        _reload = opts.reload or function()
            ---@type any
            local config = require('config')

            config:reload()
        end,

        _spawn = opts.spawn or function(body)
            fiber.create(body)
        end,

        _sleep = opts.sleep or fiber.sleep,
    }, Module)
end

--- Достаёт раздел config.etcd из конфигурации инстанса.
---@param iconfig table|nil
---@return table|nil
local function read_etcd_section(iconfig)
    if type(iconfig) ~= 'table' then
        return nil
    end

    local config_node = iconfig.config
    if type(config_node) ~= 'table' then
        return nil
    end

    local etcd = config_node.etcd
    if type(etcd) ~= 'table' then
        return nil
    end

    return etcd
end

--- Длительность из конфигурации: число секунд либо строка с единицей,
--- как принимает ядро. Ядро проверяет запись до вызова источника, так
--- что негодная строка здесь — ошибка вызывающего, а не отказ чтения.
---@param value any Значение из конфигурации
---@param path string Путь настройки — для текста отказа
---@return number|nil seconds nil, если настройка не задана
local function read_duration(value, path)
    if value == nil then
        return nil
    end

    local seconds, err = units.parse_duration(value)
    if seconds == nil then
        raise(('[tnt.ce.etcd] %s: %s'):format(path, tostring(err)))
    end

    return seconds
end

---@class TntEtcdRequestSettings
---@field timeout number Срок запроса в секундах
---@field unix_socket string|nil Unix-сокет вместо сети
---@field interface string|nil Исходящий сетевой интерфейс
---@field verbose boolean|nil Подробный вывод curl на stderr

--- Настройки HTTP-запроса из config.etcd.http.request.
---@param etcd table
---@return TntEtcdRequestSettings
local function read_request(etcd)
    local request = type(etcd.http) == 'table' and etcd.http.request or nil
    if type(request) ~= 'table' then
        return { timeout = DEFAULT_TIMEOUT }
    end

    return {
        timeout = read_duration(request.timeout, 'config.etcd.http.request.timeout') or DEFAULT_TIMEOUT,
        unix_socket = request.unix_socket,
        interface = request.interface,
        verbose = request.verbose,
    }
end

---@class TntEtcdWatchers
---@field reconnect_timeout number|nil Пауза перед новой попыткой после молчания хранилища
---@field reconnect_max_attempts number|nil Сколько молчаний подряд терпеть, прежде чем остановиться

--- Настройки наблюдения из config.etcd.watchers.
---
--- В Enterprise это сроки переподключения потока наблюдения; здесь
--- наблюдение опросом, и они значат то же для опроса: пауза после
--- неудачной попытки и предел неудач подряд.
---@param etcd table
---@return TntEtcdWatchers
local function read_watchers(etcd)
    local watchers = type(etcd.watchers) == 'table' and etcd.watchers or {}

    return {
        reconnect_timeout = read_duration(watchers.reconnect_timeout, 'config.etcd.watchers.reconnect_timeout'),
        reconnect_max_attempts = watchers.reconnect_max_attempts,
    }
end

--- Ключи, по которым лежит конфигурация кластера.
---
--- Основной — `<prefix>/config/all`: под этим ключом конфигурацию кладёт
--- `tt cluster publish`, и его же читает Enterprise. Точнее, Enterprise
--- читает всю ветку `<prefix>/config/` целиком и сливает значения её
--- ключей, а здесь читается один ключ — тот, что пишет `tt`. На переезде
--- разница не сказывается, пока в ветке нет ничего, кроме конфигурации:
--- своё рядом с ней держат вне ветки (историю правок, например,
--- в `<prefix>/history/<ревизия>`), иначе Enterprise слил бы это
--- в конфигурацию и отверг как неизвестные поля.
--- Старый ключ `<prefix>/config` — наше прошлое, а не ядра:
--- Enterprise его не читает, и развёртывание на нём переезжает только
--- после переноса конфигурации под основной ключ. Оставлен, чтобы такие
--- развёртывания не требовали разовой миграции ради обновления пакета.
---@param prefix string
---@return string primary
---@return string legacy
local function config_keys(prefix)
    -- Хвостовые косые снимает rstrip, а не замена по образцу: у образца
    -- мутанты повтора (минус и звёздочка на месте плюса) снимали те же
    -- знаки и были неотличимы.
    local trimmed = prefix:rstrip('/')

    return trimmed .. '/config/all', trimmed .. '/config'
end

--- Разбирает тело конфигурации.
---@param body string
---@return table|nil parsed
---@return string|nil err
local function parse_config(body)
    local ok, parsed = pcall(yaml.decode, body)

    if not ok or type(parsed) ~= 'table' then
        return nil,
            ('конфигурация из etcd не разобрана как YAML: %s'):format(tostring(parsed))
    end

    return parsed, nil
end

--- Рассказывает ядру ревизию прочитанной конфигурации.
---
--- Ядро складывает сведения источников в `config:info('v2').meta` по имени
--- источника: `last` — что прочитано, `active` — что применено, и второе
--- оно переписывает первым только после удачного применения. Так узел
--- отличает ревизию, которую прочитал, от той, на которой живёт, — а
--- раскатка и правило `config_revision` спрашивают именно вторую. Без
--- рассказа ядро о ревизии не знает вовсе: источник для него безымянен.
---
--- Рассказывается ревизия ключа, а не всего хранилища: номера узлов
--- сравниваются между собой, а ревизию хранилища двигает любая запись
--- в etcd. Встроенный источник Enterprise кладёт под `revision` ревизию
--- хранилища, а ревизии ключей — отдельно, в `mod_revision`. У нас номер
--- один — тот, по которому судят; раскатке он годится так же: её запись
--- получает ревизию, которая и становится ревизией ключа.
---
--- Метод ядра внутренний, как и вся дверь для сторонних источников; на
--- ядре без него источник просто молчит. В проверках вместо ядра приходит
--- nil, и молчание там — норма.
---@param config table|nil Модуль конфигурации, которому ядро велело читать
---@param name string Имя источника
---@param revision number|nil Ревизия прочитанной конфигурации
local function report_revision(config, name, revision)
    if revision == nil or type(config) ~= 'table' or type(config._meta) ~= 'function' then
        return
    end

    config:_meta(name, 'revision', revision)
end

--- Принимает успешно прочитанную конфигурацию.
---@param key string Ключ, с которого она прочитана: за ним и следит наблюдение
---@param body string
---@param meta table|nil
---@param settings TntCeEtcdSnapshotSettings Снимок на это чтение: куда его писать
function Module:_accept(key, body, meta, settings)
    local parsed, err = parse_config(body)
    if parsed == nil then
        raise(('[tnt.ce.etcd] %s'):format(err))
    end

    self._payload = parsed
    self._key = key
    self._revision = meta and meta.mod_revision or nil
    self._endpoint = meta and meta.endpoint or nil
    -- Время чтения — по настоящим стенным часам, а не `fiber.time`: тот —
    -- отметка цикла событий, и разбор большой конфигурации без уступки
    -- отодвинул бы его назад на всё своё время.
    self._synced_at = clock.realtime()
    self._stale = false

    local saved, save_error = snapshot.save(body, settings.path)
    if not saved then
        self._log.warn('снимок конфигурации не сохранён', { err = save_error })
    end
end

--- Пытается подняться на локальном снимке.
---@param reason string Почему не удалось прочитать etcd
---@param settings TntCeEtcdSnapshotSettings Снимок на это чтение: прочитан до похода в etcd
function Module:_fall_back(reason, settings)
    if not settings.fallback then
        raise(
            ('[tnt.ce.etcd] etcd недоступен, откат на снимок выключен: %s'):format(
                reason
            )
        )
    end

    local body, load_error = snapshot.load(settings.path)
    if body == nil then
        raise(
            ('[tnt.ce.etcd] etcd недоступен и снимка нет, стартовать не на чем.\n%s\n%s'):format(
                reason,
                tostring(load_error)
            )
        )
    end

    local parsed, parse_error = parse_config(body)
    if parsed == nil then
        raise(('[tnt.ce.etcd] снимок повреждён: %s'):format(tostring(parse_error)))
    end

    self._payload = parsed
    self._key = nil
    self._revision = nil
    self._endpoint = nil
    self._synced_at = snapshot.saved_at(settings.path)
    self._stale = true

    self._log.warn('etcd недоступен, инстанс поднимается на снимке', {
        snapshot = settings.path,
        reason = reason,
    })
end

--- Снимок на одно чтение конфигурации: где он лежит и разрешён ли откат.
---
--- Каталог узла спрашивается у той конфигурации, что собрали источники
--- до etcd, а имя инстанса — у ядра: до `box.cfg` его больше знать
--- некому. Поле `_instance_name` внутреннее, как и вся дверь для
--- сторонних источников; в проверках вместо ядра приходит nil.
---
--- Каталог, который до `box.cfg` не определить, — не отказ: узел
--- поднимается, как поднялся бы без `process.work_dir`, от текущего
--- каталога, а журнал называет причину — иначе оператор, положивший
--- `.env` в рабочий каталог, не узнал бы, что старт его не прочитал.
---@param config table|nil Модуль конфигурации, которому ядро велело читать
---@param iconfig table|nil Конфигурация инстанса, собранная до etcd
---@return TntCeEtcdSnapshotSettings
function Module:_snapshot_settings(config, iconfig)
    local instance_name = type(config) == 'table' and config._instance_name or nil
    local directory, err = snapshot.directory(iconfig, instance_name)

    if err ~= nil then
        self._log.warn(
            'каталог узла не определён: .env и снимок считаются от текущего',
            { err = err }
        )
    end

    return snapshot.settings(directory)
end

---@class TntEtcdRequest
---@field client TntCeEtcdClient Клиент etcd
---@field endpoints string[] Адреса хранилища по порядку
---@field options TntCeEtcdFetchOptions Учётные данные, TLS и настройки запроса
---@field primary_key string Основной ключ конфигурации
---@field legacy_key string Старый ключ конфигурации

--- Читает конфигурацию из хранилища, а если оно не ответило — со снимка.
---
--- Основной ключ читается первым, старый — только если основной
--- не прочитан. Ключа нет ни там, ни там — это не отказ связи, а пустая
--- конфигурация: кластер поднимется из других источников, а конфигурация
--- в etcd появится при первой записи.
---@param request TntEtcdRequest
---@param settings TntCeEtcdSnapshotSettings Снимок на это чтение
function Module:_read(request, settings)
    local client, endpoints, options = request.client, request.endpoints, request.options

    local body, err, meta = client:fetch(endpoints, request.primary_key, options)
    if body ~= nil then
        self:_accept(request.primary_key, body, meta, settings)

        return
    end

    local primary_error, primary_meta = err, meta
    body, err, meta = client:fetch(endpoints, request.legacy_key, options)
    if body ~= nil then
        self:_accept(request.legacy_key, body, meta, settings)

        return
    end

    local primary_missing = primary_meta ~= nil and primary_meta.missing == true
    local legacy_missing = meta ~= nil and meta.missing == true
    local both_missing = primary_missing and legacy_missing
    if both_missing then
        self._log.info('конфигурации в etcd нет, ключ пуст', { key = request.primary_key })
        self._payload = {}
        self._key = nil
        self._revision = nil
        self._synced_at = clock.realtime()
        self._stale = false

        return
    end

    self:_fall_back(('%s\n%s'):format(tostring(primary_error), tostring(err)), settings)
end

--- Читает конфигурацию. Вызывается ядром при каждом чтении конфигурации.
---@param config table|nil Модуль конфигурации: ему источник рассказывает ревизию
---@param iconfig table Конфигурация инстанса, собранная предыдущими источниками
function Module:sync(config, iconfig)
    local etcd = read_etcd_section(iconfig)

    if etcd == nil then
        -- Раздела нет — источнику нечего добавить. Пустая таблица, а не nil:
        -- слияние конфигураций отвергает nil с невнятной ошибкой типа.
        self._payload = {}
        return
    end

    -- Выключатель отката читается здесь, до похода в хранилище, а не
    -- в миг отказа: негодное значение обязано остановить старт сразу,
    -- а не всплыть в тот час, когда etcd лёг и откат нужнее всего. Путь
    -- снимка берётся тем же прочтением `.env`: одно чтение конфигурации
    -- ищет, пишет и называет в журнале один и тот же снимок.
    local settings = self:_snapshot_settings(config, iconfig)

    local endpoints = etcd.endpoints
    if type(endpoints) ~= 'table' or endpoints[1] == nil then
        raise('[tnt.ce.etcd] config.etcd.endpoints должен быть непустым списком')
    end

    local prefix = etcd.prefix
    if type(prefix) ~= 'string' or prefix == '' then
        raise('[tnt.ce.etcd] config.etcd.prefix обязателен')
    end

    local request_settings = read_request(etcd)
    local primary_key, legacy_key = config_keys(prefix)

    -- Как ходить в хранилище, известно только отсюда: наблюдение получает
    -- те же адреса, ключи и учётные данные, что и само чтение.
    ---@type TntEtcdRequest
    local request = {
        client = self._client_factory({ timeout = request_settings.timeout }),
        endpoints = endpoints,
        options = {
            username = etcd.username,
            password = etcd.password,
            ssl = type(etcd.ssl) == 'table' and etcd.ssl or nil,
            timeout = request_settings.timeout,
            unix_socket = request_settings.unix_socket,
            interface = request_settings.interface,
            verbose = request_settings.verbose,
        },
        primary_key = primary_key,
        legacy_key = legacy_key,
    }

    self._watchers = read_watchers(etcd)
    self._watch_request = request

    self:_read(request, settings)
    report_revision(config, self.name, self._revision)

    -- Наблюдение заводит всякое чтение, кроме отказа, — и на снимке, и без
    -- ключа. Узлу на снимке оно нужнее всех: иначе, поднятый при лежащем
    -- etcd, он жил бы на снимке, сколько бы etcd ни работал снова, а узел,
    -- поднятый до первой записи конфигурации, так её и не увидел бы — оба
    -- до ручного перечитывания.
    self:watch()
end

--- Начинает следить за ключом конфигурации. Как и почему опросом —
--- в `tnt.ce.etcd.watch`.
function Module:watch()
    watch.start(self)
end

--- Прекращает следить за ключом.
function Module:unwatch()
    self._watching = false
end

--- Отдаёт прочитанную конфигурацию.
--- Пустая таблица вместо nil обязательна: слияние конфигураций отвергает nil.
---@return table
function Module:get()
    return self._payload or {}
end

---@class TntEtcdSourceStatus
---@field synced_at number|nil Когда прочитали, по стенным часам; на снимке — время его правки
---@field revision number|nil Ревизия прочитанного ключа; на снимке и без ключа — нет
---@field endpoint string|nil Адрес, который ответил
---@field stale boolean Конфигурация взята из снимка
---@field ready boolean Конфигурация прочитана — из etcd, снимка или пустая
---@field watching boolean Идёт ли наблюдение

--- Состояние источника: когда читали, откуда и не устарела ли конфигурация.
---
--- Ревизия здесь — прочитанная, ревизия ключа. Применена ли она, знает
--- только ядро: об этом говорит `config:info('v2').meta.active`.
---@return TntEtcdSourceStatus
function Module:status()
    return {
        synced_at = self._synced_at,
        revision = self._revision,
        endpoint = self._endpoint,
        stale = self._stale,
        ready = self._payload ~= nil,
        watching = self._watching,
    }
end

return Module
