--- Рабочий каталог узла на живом ядре 3.8: какой `.env` видят каркас
--- расширений и источник etcd, когда `process.work_dir` отличен от
--- каталога запуска.
---
--- Двойник показывает, что источник приписывает каталог, но не главное:
--- что ядро зовёт источник по обе стороны `box.cfg` — на старте, ещё раз
--- сразу после него на перезапуске узла с данными и на каждом
--- `config:reload()`, — а каркас однажды и раньше всех. Показать это
--- может только настоящее ядро.
---
--- etcd здесь нет нарочно: адрес указывает на закрытый порт, узел
--- поднимается на снимке, и снимок с выключателем отката называют, чей
--- `.env` прочитан. Два файла сказаны так, чтобы всякое неверное прочтение
--- роняло подъём: `.env` каталога запуска выключает откат, а `.env`
--- рабочего каталога просит расширение, которого нет.

local t = require('luatest')
local fio = require('fio')

local helper = dofile('test/helper.lua')
local testing = helper.testing

local g = t.group('tnt.ce.etcd.work_dir.live')

--- Имя инстанса.
local NAME = 'work-dir-001-a'

--- Каталог запуска и конфигурация узла в нём; рабочий каталог — `work`
--- внутри, относительный: такой ядро считает от каталога запуска, и после
--- перехода в него приписывать его второй раз нельзя.
---@return string launch Каталог запуска
---@return string work Рабочий каталог узла
local function prepared()
    local launch = fio.tempdir()
    local work = fio.pathjoin(launch, 'work')

    fio.mktree(work)

    testing.write_file(fio.pathjoin(launch, 'config.yaml'), table.concat({
        'credentials: {users: {guest: {roles: [super]}}}',
        'process: {work_dir: work}',
        'config:',
        '  etcd:',
        ("    endpoints: ['http://127.0.0.1:%d']"):format(testing.free_port()),
        '    prefix: /tnt-work-dir',
        '    http: {request: {timeout: 1}}',
        ('groups: {g: {replicasets: {r: {instances: {%s: {'):format(NAME),
        ("  iproto: {listen: [{uri: 'unix/:%s/instance.iproto'}]},"):format(launch),
        '}}}}}}',
    }, '\n') .. '\n')

    -- Сценарий зовётся после применения конфигурации, и путь к нему
    -- абсолютный: к этому мигу процесс уже стоит в рабочем каталоге.
    testing.write_file(fio.pathjoin(launch, 'init.lua'), "require('strict').on()\n_G.ready = true\n")

    testing.write_file(fio.pathjoin(launch, '.env'), 'TNT_CE_EXTENSIONS=tnt.ce.etcd\nTNT_CE_ETCD_FALLBACK=off\n')
    testing.write_file(
        fio.pathjoin(work, '.env'),
        'TNT_CE_EXTENSIONS=нет.такого.расширения\nTNT_CE_ETCD_SNAPSHOT=снимок.yaml\n'
    )
    testing.write_file(fio.pathjoin(work, 'снимок.yaml'), 'roles_cfg: {marker: work}\n')

    return launch, work
end

--- Что узел взял и откуда: метка снимка, стоит ли он в рабочем каталоге,
--- какие расширения завёл.
---
--- Каталог сверяется узлом по номеру узла файловой системы, а не строкой
--- пути: на macOS `/tmp` — ссылка на `/private/tmp`, и одинаковый каталог
--- назывался бы по-разному.
---@param server table
---@param work string Рабочий каталог узла
---@return table
local function observed(server, work)
    return server:exec(function(expected)
        local files = require('fio')
        local extras = require('tnt.ce.extras')
        ---@type any
        local config = require('config')

        return {
            marker = config:get('roles_cfg.marker'),
            in_work_dir = files.stat(files.cwd()).ino == files.stat(expected).ino,
            extensions = extras.registered_names(),
            stale = extras.status().sources.etcd.stale,
        }
    end, { work })
end

g.after_each(function()
    if g.server ~= nil then
        g.server:drop()
        g.server = nil
    end

    if g.launch ~= nil then
        fio.rmtree(g.launch)
        g.launch = nil
    end
end)

g.test_node_reads_its_work_dir_env_file_on_every_side_of_box_cfg = function()
    local launch, work = prepared()

    g.launch = launch

    -- Старт: список расширений — из `.env` каталога запуска, выключатель
    -- и снимок — из `.env` рабочего каталога, хотя процесс ещё стоит
    -- в каталоге запуска. Прочти источник файл запуска, откат был бы
    -- выключен, и узел без etcd не поднялся бы.
    g.server = helper.start_in(launch, NAME)

    local expected = {
        marker = 'work',
        in_work_dir = true,
        extensions = { 'etcd' },
        stale = true,
    }

    t.assert_equals(observed(g.server, work), expected, 'старт')

    -- Перечитывание идёт уже в рабочем каталоге и берёт тот же файл.
    g.server:exec(function()
        ---@type any
        local config = require('config')

        config:reload()
    end)

    t.assert_equals(observed(g.server, work), expected, 'перечитывание')

    -- Правка `.env` рабочего каталога действует со следующего чтения
    -- конфигурации: выключатель, сказанный там, выключает откат.
    testing.write_file(fio.pathjoin(work, '.env'), 'TNT_CE_ETCD_FALLBACK=off\n')

    local reloaded, err = g.server:exec(function()
        ---@type any
        local config = require('config')

        return pcall(config.reload, config)
    end)

    t.assert_equals(reloaded, false)
    t.assert_str_contains(tostring(err), 'откат на снимок выключен')

    -- Перезапуск узла с данными читает источник дважды: до `box.cfg`
    -- и сразу после него. Оба чтения берут `.env` рабочего каталога,
    -- а прежде первое брало файл запуска и падало на его выключателе.
    testing.write_file(fio.pathjoin(work, '.env'), 'TNT_CE_ETCD_SNAPSHOT=снимок.yaml\n')

    g.server:restart(nil, { wait_until_ready = true })

    t.assert_equals(observed(g.server, work), expected, 'перезапуск')
end
