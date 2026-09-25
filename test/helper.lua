--- Общие средства проверок пакета: исходники, оснастка, окружение
--- под подменой и узел из готового каталога запуска.
---
--- Проверки берут всё отсюда: так у каждого средства одно место, и его
--- можно подменить целиком, не трогая ни одного файла проверок.
---
--- Исходники читаются с диска, а не через `require`: у Tarantool свой
--- загрузчик `.rocks`, он идёт раньше `package.path` и подсунул бы
--- установленную копию пакета, если она есть. Проверки тогда шли бы
--- против вчерашнего кода, а покрытие считалось бы по нему. Зависимости
--- пакета — `tnt.ce.extras`, `tnt.env`, `tnt.context`, `tnt.id`,
--- `tnt.log`, `tnt.must`, `tnt.external`, `tnt.fs` — берутся из `.rocks`
--- обычным `require`: проверяется этот пакет, а не они. Оснастка
--- в `test/testing/` грузится так же и один раз на процесс: второй
--- экземпляр загрузчика не знал бы, что вытеснил первый, и не вернул бы
--- вытесненное на место.

local t = require('luatest')
local fio = require('fio')

--- Модули оснастки в порядке зависимостей.
local TESTING = {
    { name = 'tnt.testing.sources', path = 'test/testing/sources.lua' },
    { name = 'tnt.testing.files', path = 'test/testing/files.lua' },
    { name = 'tnt.testing.clock', path = 'test/testing/clock.lua' },
    { name = 'tnt.testing.node', path = 'test/testing/node.lua' },
}

for _, module in ipairs(TESTING) do
    if package.loaded[module.name] == nil then
        local chunk, failure = loadfile(fio.abspath(module.path))

        if chunk == nil then
            error(('оснастка %s не читается: %s'):format(module.name, tostring(failure)))
        end

        package.loaded[module.name] = chunk()
    end
end

local sources = package.loaded['tnt.testing.sources']

--- Корень репозитория: от него строятся пути узла.
---
--- Считается от этого файла, а не от рабочего каталога: узел живёт
--- в своём каталоге запуска, и относительный путь он не понял бы.
local this_file =
    assert(debug.getinfo(1), 'нет отладочной информации о файле').source:sub(2)
local project_root = fio.abspath(fio.pathjoin(fio.dirname(this_file), '..'))

local helper = {}

--- Оснастка проверок под теми именами, что зовут проверки: загрузка
--- и выгрузка исходников, работа без уступки, запись файла и свободный
--- порт.
helper.testing = {
    load_sources = sources.load,
    unload_sources = sources.unload,
    module = sources.module,
    work_without_yielding = package.loaded['tnt.testing.clock'].work_without_yielding,
    write_file = package.loaded['tnt.testing.files'].write,
    free_port = package.loaded['tnt.testing.node'].free_port,
}

--- Модули пакета по именам: путь складывается из имени.
---@param names string[]
---@return { name: string, path: string }[]
local function own(names)
    local list = {}

    for _, name in ipairs(names) do
        table.insert(list, { name = name, path = name:gsub('%.', '/') .. '.lua' })
    end

    return list
end

--- Клиент etcd.
helper.CLIENT = own({ 'tnt.ce.etcd.client' })

--- Снимок.
local SNAPSHOT = own({ 'tnt.ce.etcd.snapshot' })

--- Источник: грузится после снимка, которого он берёт по имени, и после
--- наблюдения, которое он берёт так же.
helper.SOURCE = own({ 'tnt.ce.etcd.watch', 'tnt.ce.etcd.source' })

--- Точка подключения пакета.
helper.ENTRY = own({ 'tnt.ce.etcd' })

--- Мир, в котором есть ровно названные переменные и файлы.
---
--- Настоящая переменная процесса пережила бы проверку и досталась
--- соседней, а настоящий `.env` рядом — чужой. Таблицы читаются
--- на каждом обращении, и проверка правит их по ходу.
---@param vars table<string, string>
---@param files table<string, string>|nil Что лежит на диске: путь — текст
---@return table
local function world(vars, files)
    local on_disk = files or {}

    return {
        getenv = function(name)
            return vars[name]
        end,

        exists = function(path)
            return on_disk[path] ~= nil
        end,

        read = function(path)
            if on_disk[path] == nil then
                return nil, 'нет такого файла'
            end

            return on_disk[path]
        end,
    }
end

--- Снимок из исходника, читающий переданное окружение.
---
--- Окружение подменяется у `tnt-env` целиком, внешней зависимостью.
--- Источник берёт снимок по имени модуля, поэтому исходник кладётся
--- в `package.loaded`: иначе `require` нашёл бы установленную копию.
---@param vars table<string, string>
---@param files table<string, string>|nil
---@return table snapshot
function helper.snapshot(vars, files)
    require('tnt.env')._set_source(world(vars, files))

    return sources.load(SNAPSHOT, 'tnt.ce.etcd.snapshot')
end

--- Окружение, которое считает прочтения файлов.
---
--- Проверке бывает нужно знать, сколько раз и какие файлы открыл пакет:
--- сколько раз `.env` читается на одно чтение конфигурации и чей именно.
---@param vars table<string, string>
---@param files table<string, string>|nil Что лежит на диске: путь — текст
---@return string[] reads Пути прочитанных файлов по порядку; пополняется
function helper.counted_environment(vars, files)
    local counted = world(vars, files)
    local read = counted.read
    local reads = {}

    counted.read = function(path)
        table.insert(reads, path)

        return read(path)
    end

    require('tnt.env')._set_source(counted)

    return reads
end

--- Файловая система, которую взял снимок: ей проверки подменяют `fio`.
---@return any
function helper.fs()
    return require('tnt.fs')
end

--- Убирает снимок из исходника и возвращает то, что он вытеснил.
---
--- Заодно снимает подмену `fio` у файловой системы: она одна на процесс,
--- из `.rocks`, и со снимком заново не грузится. Без этого отказ диска,
--- подменённый одной проверкой, достался бы всем следующим.
function helper.forget_snapshot()
    sources.unload(SNAPSHOT)
    require('tnt.fs')._set_source(nil)
end

--- Убирает подмену окружения, оставленную `snapshot`.
function helper.forget_environment()
    require('tnt.env')._set_source(nil)
end

--- Поднимает узел из готового каталога запуска: `config.yaml`, `init.lua`
--- и `.env` в нём кладёт проверка.
---
--- Прямой `t.Server:new`, а не узел оснастки: тот заводит каталог сам
--- и поднимает узел тут же, и ни рабочий каталог, ни свой `.env` ему
--- не сказать. Модули пакета узел берёт из корня репозитория,
--- зависимости — из `.rocks`. Строгий режим глобалов включает сценарий
--- первой строкой: прямой `t.Server:new` сам его не включает.
---@param launch string Каталог запуска
---@param name string Имя инстанса
---@return table server
function helper.start_in(launch, name)
    local rocks = fio.pathjoin(project_root, '.rocks')
    local paths = {
        fio.pathjoin(project_root, '?.lua'),
        fio.pathjoin(rocks, 'share', 'tarantool', '?.lua'),
        fio.pathjoin(rocks, 'share', 'tarantool', '?', 'init.lua'),
    }

    local server = t.Server:new({
        alias = name,
        command = arg[-1],
        args = { fio.pathjoin(launch, 'init.lua') },
        chdir = launch,
        net_box_uri = 'unix/:' .. fio.pathjoin(launch, 'instance.iproto'),
        setsearchroot = false,
        env = {
            TT_CONFIG = fio.pathjoin(launch, 'config.yaml'),
            TT_INSTANCE_NAME = name,
            LUA_PATH = table.concat(paths, ';') .. ';',
            LUA_CPATH = fio.pathjoin(rocks, 'lib', 'tarantool', '?.so') .. ';',
        },
    })

    server:start({ wait_until_ready = true })

    return server
end

--- Проверяет, что объект отвечает контракту источника конфигурации:
--- ядро требует поля name и type и методы sync и get.
---@param source table
function helper.assert_source_contract(source)
    t.assert_equals(source.name, 'etcd')
    t.assert_equals(source.type, 'cluster')
    t.assert_type(source.sync, 'function')
    t.assert_type(source.get, 'function')
end

return helper
