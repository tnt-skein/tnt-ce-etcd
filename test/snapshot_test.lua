--- Тесты локального снимка конфигурации. Работают с настоящими файлами
--- во временном каталоге: проверяется именно поведение на диске.

local t = require('luatest')
local errno = require('errno')
local fiber = require('fiber')
local fio = require('fio')

local helper = dofile('test/helper.lua')

local g = t.group('tnt.ce.etcd.snapshot')

--- Окружение, которое видит снимок; своё на каждую проверку.
---@type table<string, string>
local vars

--- Загружает модуль и выбирает снимку место во временном каталоге.
---
--- Путь идёт и в окружение — так его увидит `settings`, — и в ответ:
--- записи и чтению снимка его передают аргументом.
---@return table snapshot
---@return string path
local function load_snapshot()
    local directory = fio.tempdir()
    local path = fio.pathjoin(directory, 'snapshot.yaml')

    vars.TNT_CE_ETCD_SNAPSHOT = path

    return helper.snapshot(vars), path
end

g.before_each(function()
    vars = {}
end)

g.after_each(function()
    helper.forget_environment()
    helper.forget_snapshot()
end)

-- Путь берётся из окружения.
g.test_path_comes_from_environment = function()
    local snapshot, path = load_snapshot()

    t.assert_equals(snapshot.settings().path, path)
end

-- Без переменной путь по умолчанию лежит рядом с данными инстанса.
g.test_default_path = function()
    local snapshot = helper.snapshot(vars)

    t.assert_equals(snapshot.settings().path, 'var/etcd-config-snapshot.yaml')
end

-- Пустая переменная равносильна её отсутствию.
g.test_blank_path_falls_back_to_default = function()
    local snapshot = helper.snapshot(vars)

    for _, blank in ipairs({ '', 'empty', 'null' }) do
        vars.TNT_CE_ETCD_SNAPSHOT = blank

        t.assert_equals(snapshot.settings().path, 'var/etcd-config-snapshot.yaml', 'значение ' .. blank)
    end
end

-- Путь из `.env` действует, когда в окружении процесса его нет.
g.test_path_comes_from_the_env_file = function()
    local snapshot = helper.snapshot(vars, { ['.env'] = 'TNT_CE_ETCD_SNAPSHOT=/из/файла.yaml\n' })

    t.assert_equals(snapshot.settings().path, '/из/файла.yaml')
end

-- Одно чтение окружения отвечает на оба вопроса разом: `.env` открыт
-- однажды, и путь с выключателем взяты из одного его прочтения.
g.test_one_reading_answers_both_questions = function()
    local snapshot = helper.snapshot(vars)
    local reads = helper.counted_environment(vars, {
        ['.env'] = 'TNT_CE_ETCD_SNAPSHOT=/из/файла.yaml\nTNT_CE_ETCD_FALLBACK=off\n',
    })

    t.assert_equals(snapshot.settings(), { path = '/из/файла.yaml', fallback = false })
    t.assert_equals(reads, { '.env' })
end

-- `.env` перечитывается на каждое чтение окружения: общего чтения на
-- процесс снимок не заводит — его зовут до `box.cfg`, раньше приложения,
-- — а правка файла действует со следующего чтения конфигурации.
g.test_env_file_is_read_anew_on_every_reading = function()
    local files = { ['.env'] = 'TNT_CE_ETCD_SNAPSHOT=/первый.yaml\n' }
    local snapshot = helper.snapshot(vars, files)

    t.assert_equals(snapshot.settings(), { path = '/первый.yaml', fallback = true })

    files['.env'] = 'TNT_CE_ETCD_SNAPSHOT=/второй.yaml\nTNT_CE_ETCD_FALLBACK=off\n'

    t.assert_equals(snapshot.settings(), { path = '/второй.yaml', fallback = false })
end

-- ── Каталог узла ─────────────────────────────────────────────────────

--- Конфигурация инстанса с рабочим каталогом.
---@param work_dir any
---@return table
local function with_work_dir(work_dir)
    return { process = { work_dir = work_dir } }
end

-- Без `process.work_dir` пути считаются от текущего каталога: его
-- не называет ни конфигурация, ни пустота от YAML.
g.test_without_work_dir_the_directory_is_the_current_one = function()
    local snapshot = helper.snapshot(vars)
    local cases = {
        ['пустая конфигурация'] = {},
        ['process не раздел'] = { process = 'строка' },
        ['work_dir не задан'] = with_work_dir(nil),
        ['work_dir пуст в YAML'] = with_work_dir(box.NULL),
    }

    t.assert_equals({ snapshot.directory(nil, 'etcd-001-a') }, {}, 'конфигурации нет')

    for label, iconfig in pairs(cases) do
        t.assert_equals({ snapshot.directory(iconfig, 'etcd-001-a') }, {}, label)
    end
end

-- До первого `box.cfg` каталог узла — `process.work_dir` как задан:
-- относительный считается от каталога запуска, в котором процесс ещё стоит.
g.test_before_box_cfg_the_directory_is_work_dir = function()
    local snapshot = helper.snapshot(vars)

    t.assert_equals({ snapshot.directory(with_work_dir('/srv/узел')) }, { '/srv/узел' })
    t.assert_equals({ snapshot.directory(with_work_dir('work')) }, { 'work' })
end

-- После первого `box.cfg` процесс уже стоит в рабочем каталоге, и пути
-- считаются от текущего: приписать каталог второй раз значило бы уйти
-- в `work/work`.
g.test_after_box_cfg_the_directory_is_the_current_one = function()
    local snapshot = helper.snapshot(vars)

    snapshot._set_source({
        configured = function()
            return true
        end,
    })

    t.assert_equals({ snapshot.directory(with_work_dir('work'), 'etcd-001-a') }, {})
end

-- Имя инстанса ядро знает с запуска, и подстановка раскрывается в любой
-- записи — с пробелами и без, сколько бы раз ни встретилась.
g.test_instance_name_is_substituted = function()
    local snapshot = helper.snapshot(vars)
    local iconfig = with_work_dir('var/lib/{{ instance_name }}/{{instance_name}}')

    t.assert_equals({ snapshot.directory(iconfig, 'etcd-001-a') }, { 'var/lib/etcd-001-a/etcd-001-a' })
end

-- Подстановку, которой до `box.cfg` не раскрыть, каталогом не выдают:
-- путь с фигурными скобками создал бы на диске каталог с таким именем.
g.test_unresolvable_work_dir_is_refused = function()
    local snapshot = helper.snapshot(vars)
    local refusal = 'process.work_dir «%s»: до box.cfg раскрывается только {{ instance_name }}'

    for _, case in ipairs({
        { '{{ replicaset_name }}/{{ instance_name }}', 'etcd-001-a' },
        { 'var/{{ context.dir }}', 'etcd-001-a' },
        -- Имени ядро не назвало: и своя подстановка остаётся нераскрытой.
        { 'var/{{ instance_name }}', nil },
    }) do
        local work_dir = case[1]

        t.assert_equals({ snapshot.directory(with_work_dir(work_dir), case[2]) }, { nil, refusal:format(work_dir) })
    end
end

-- Окружение и путь снимка берутся в каталоге узла: `.env` каталога
-- запуска при этом не открывается вовсе.
g.test_settings_are_read_in_the_node_directory = function()
    local snapshot = helper.snapshot(vars)
    local reads = helper.counted_environment(vars, {
        ['.env'] = 'TNT_CE_ETCD_FALLBACK=выкл\n',
        ['/srv/узел/.env'] = 'TNT_CE_ETCD_SNAPSHOT=var/снимок.yaml\nTNT_CE_ETCD_FALLBACK=off\n',
    })

    t.assert_equals(
        snapshot.settings('/srv/узел'),
        { path = '/srv/узел/var/снимок.yaml', fallback = false }
    )
    t.assert_equals(reads, { '/srv/узел/.env' })
end

-- Относительный каталог считается от текущего, умолчание пути ложится
-- в него же, а `..` снимается сразу: рабочего каталога на первом
-- старте ещё может не быть, и путь через него не нашёлся бы на диске.
g.test_relative_directory_is_resolved_and_normalized = function()
    local snapshot = helper.snapshot(vars)
    local work = fio.pathjoin(fio.cwd(), 'work')

    helper.counted_environment(vars, { [work .. '/.env'] = 'TNT_CE_ETCD_FALLBACK=off\n' })

    t.assert_equals(snapshot.settings('work'), { path = work .. '/var/etcd-config-snapshot.yaml', fallback = false })

    vars.TNT_CE_ETCD_SNAPSHOT = '../общий.yaml'

    t.assert_equals(snapshot.settings('work').path, fio.pathjoin(fio.cwd(), 'общий.yaml'))
end

-- Абсолютный путь снимка каталогом узла не трогается.
g.test_absolute_path_ignores_the_node_directory = function()
    vars.TNT_CE_ETCD_SNAPSHOT = '/var/lib/общий/снимок.yaml'

    t.assert_equals(helper.snapshot(vars).settings('/srv/узел').path, '/var/lib/общий/снимок.yaml')
end

-- ── Файл снимка ──────────────────────────────────────────────────────

--- Подменяет в `fio` у файловой системы снимка названные действия;
--- прочее — настоящее.
---
--- Подмена живёт до конца проверки: следующая грузит снимок вместе
--- с файловой системой заново.
---@param replaced table<string, function>
local function with_fio(replaced)
    helper.fs()._set_source({ fio = setmetatable(replaced, { __index = fio }) })
end

--- Открытие настоящего файла, у ручки которого отказывает одно действие.
---
--- Файл настоящий: проверке важно, что временный файл подмены заведён
--- на диске и после отказа убран.
---@param action string Действие ручки: write либо fsync
---@param code integer Код отказа
---@return table<string, function>
local function refusing(action, code)
    return {
        open = function(name, flags, mode)
            local handle, err = fio.open(name, flags, mode)

            if handle == nil then
                return nil, err
            end

            local broken = {}

            broken[action] = function()
                return false, { errno = code }
            end

            return setmetatable(broken, {
                __index = function(_, other)
                    return function(_, ...)
                        return handle[other](handle, ...)
                    end
                end,
            })
        end,
    }
end

--- Имена в каталоге снимка по порядку, со скрытыми: временный файл
--- подмены — скрытый.
---@param path string Путь к снимку
---@return string[]
local function beside(path)
    local names = assert(fio.listdir(fio.dirname(path)))

    table.sort(names)

    return names
end

-- Сохранённое читается обратно без изменений.
g.test_save_and_load_roundtrip = function()
    local snapshot, path = load_snapshot()

    t.assert_equals({ snapshot.save('groups: {}\n', path) }, { true })
    t.assert_equals({ snapshot.load(path) }, { 'groups: {}\n' })
end

-- Каталог для снимка создаётся сам.
g.test_save_creates_directory = function()
    local directory = fio.pathjoin(fio.tempdir(), 'вложенный', 'каталог')
    local snapshot = helper.snapshot(vars)

    t.assert_equals(snapshot.save('x: 1', fio.pathjoin(directory, 'snapshot.yaml')), true)
    t.assert_equals(fio.path.is_dir(directory), true)
end

-- Повторное сохранение заменяет содержимое целиком, и после двух
-- сохранений подряд в каталоге лежит ровно один файл — сам снимок:
-- временный переименован на место.
g.test_save_overwrites = function()
    local snapshot, path = load_snapshot()

    t.assert_equals(snapshot.save('первая версия', path), true)
    t.assert_equals(snapshot.save('вторая', path), true)

    t.assert_equals(snapshot.load(path), 'вторая')
    t.assert_equals(beside(path), { 'snapshot.yaml' })
end

-- Два сохранения разом не пишут в один временный файл: каждое идёт
-- в свой, оба удаются, и на месте снимка лежит одно из тел целиком,
-- а не начало короткого поверх хвоста длинного.
g.test_concurrent_saves_do_not_collide = function()
    local snapshot, path = load_snapshot()
    local bodies = { 'первая версия, длиннее второй', 'вторая' }
    local savers = {}

    for index, body in ipairs(bodies) do
        savers[index] = fiber.new(snapshot.save, body, path)
        savers[index]:set_joinable(true)
    end

    for _, saver in ipairs(savers) do
        t.assert_equals({ saver:join() }, { true, true })
    end

    t.assert_items_include(bodies, { snapshot.load(path) })
    t.assert_equals(beside(path), { 'snapshot.yaml' })
end

-- Путь без каталога сохраняется в текущий: создавать нечего.
g.test_save_to_bare_filename = function()
    local directory = fio.tempdir()
    local previous = fio.cwd()

    -- Исходники грузятся до смены каталога: пути к ним относительные.
    local snapshot = helper.snapshot(vars)

    fio.chdir(directory)

    t.assert_equals(snapshot.save('x: 1', 'snapshot.yaml'), true)
    t.assert_equals(snapshot.load('snapshot.yaml'), 'x: 1')

    fio.chdir(previous)
end

-- Каталог снимка не создать — на его месте файл: отказ называет каталог
-- и причину.
g.test_save_reports_directory_failure = function()
    local snapshot, path = load_snapshot()
    local occupied = fio.pathjoin(fio.dirname(path), 'занято')
    local file = fio.open(occupied, { 'O_WRONLY', 'O_CREAT' }, tonumber('644', 8))

    file:close()

    local refusal = ('не удалось создать каталог %s: %s'):format(
        occupied,
        errno.strerror(errno.EEXIST)
    )

    t.assert_equals({ snapshot.save('x: 1', fio.pathjoin(occupied, 'snapshot.yaml')) }, { false, refusal })
end

-- Кончилось место посреди записи: отказ с причиной, прежний снимок цел,
-- а обрезанный временный файл убран.
g.test_save_reports_write_failure = function()
    local snapshot, path = load_snapshot()

    snapshot.save('прежний', path)
    with_fio(refusing('write', errno.ENOSPC))

    t.assert_equals(
        { snapshot.save('x: 1', path) },
        { false, ('не удалось записать %s: %s'):format(path, errno.strerror(errno.ENOSPC)) }
    )
    t.assert_equals(snapshot.load(path), 'прежний')
    t.assert_equals(beside(path), { 'snapshot.yaml' })
end

-- Отказ сброса на диск — тоже отказ записи, и причину его `fio` отдаёт
-- объектом ошибки: файл, оставшийся в кэше страниц, не переживёт
-- отключения питания, и ставить его на место снимка нельзя.
g.test_save_reports_fsync_failure = function()
    local snapshot, path = load_snapshot()

    snapshot.save('прежний', path)
    with_fio(refusing('fsync', errno.EIO))

    t.assert_equals(
        { snapshot.save('x: 1', path) },
        { false, ('не удалось записать %s: %s'):format(path, errno.strerror(errno.EIO)) }
    )
    t.assert_equals(snapshot.load(path), 'прежний')
    t.assert_equals(beside(path), { 'snapshot.yaml' })
end

-- На месте снимка — каталог с содержимым: переименовать поверх него
-- нельзя. Отказ называет причину, а временный файл убран.
g.test_save_reports_rename_failure = function()
    local snapshot, path = load_snapshot()

    fio.mktree(fio.pathjoin(path, 'внутри'))

    local refusal = ('не удалось записать %s: %s'):format(path, errno.strerror(errno.EISDIR))

    t.assert_equals({ snapshot.save('x: 1', path) }, { false, refusal })
    t.assert_equals(beside(path), { 'snapshot.yaml' })
end

-- Отказ чтения — не «снимка нет»: отказ называет причину.
g.test_load_reports_read_failure = function()
    local snapshot, path = load_snapshot()

    snapshot.save('x: 1', path)
    with_fio({
        open = function()
            return nil, { errno = errno.EACCES }
        end,
    })

    t.assert_equals(
        { snapshot.load(path) },
        { nil, ('не удалось прочитать %s: %s'):format(path, errno.strerror(errno.EACCES)) }
    )
end

-- Снимок создаётся с правами, разрешающими чтение владельцу и группе.
g.test_snapshot_file_permissions = function()
    local snapshot, path = load_snapshot()
    -- Маска процесса снята: при обычной 022 умолчание fio 0666 урезалось бы
    -- до тех же 0644, и режим, потерянный по дороге в fio.open, был бы
    -- не виден. Маска общая на процесс, поэтому запись идёт под защитой:
    -- маска возвращается и тогда, когда запись бросила исключение.
    local previous = fio.umask(0)
    local called, saved, err = pcall(snapshot.save, 'x: 1', path)

    fio.umask(previous)

    t.assert_equals({ called, saved, err }, { true, true, nil })

    local stat = fio.stat(path)
    local mask = tonumber('777', 8) or 0
    local mode = bit.band(math.floor(tonumber(stat.mode) or 0), mask)
    t.assert_equals(mode, tonumber('644', 8))
end

-- Отсутствующий снимок читается как ошибка, а не как пустая конфигурация.
g.test_load_missing_snapshot = function()
    local snapshot, path = load_snapshot()

    local body, err = snapshot.load(path)

    t.assert_equals(body, nil)
    t.assert_equals(err, ('снимок %s не найден'):format(path))
end

-- Пустой снимок считается непригодным.
g.test_load_empty_snapshot = function()
    local snapshot, path = load_snapshot()

    local file = fio.open(path, { 'O_WRONLY', 'O_CREAT' }, tonumber('644', 8))
    file:close()

    t.assert_equals({ snapshot.load(path) }, { nil, ('снимок %s пуст'):format(path) })
end

-- Время сохранения — время правки файла в целых секундах, и его нет,
-- пока снимка нет.
g.test_saved_at = function()
    local snapshot, path = load_snapshot()

    t.assert_equals(snapshot.saved_at(path), nil)

    snapshot.save('x: 1', path)

    t.assert_equals(snapshot.saved_at(path), math.floor(fio.stat(path).mtime))
    t.assert_almost_equals(snapshot.saved_at(path), os.time(), 5)
end

-- Снимок, который не опросить, — снимок неизвестной давности: пустота,
-- а не ноль, который прочитался бы как 1970 год.
g.test_saved_at_of_an_unreadable_snapshot_is_unknown = function()
    local snapshot, path = load_snapshot()

    snapshot.save('x: 1', path)
    with_fio({
        stat = function()
            return nil, { errno = errno.EACCES }
        end,
    })

    t.assert_equals(snapshot.saved_at(path), nil)
end

-- Откат разрешён по умолчанию.
g.test_fallback_enabled_by_default = function()
    local snapshot = load_snapshot()

    t.assert_equals(snapshot.settings().fallback, true)
end

-- Откат выключается любым из привычных написаний.
g.test_fallback_can_be_disabled = function()
    local snapshot = load_snapshot()

    for _, value in ipairs({ 'off', 'OFF', 'false', '0', 'no' }) do
        vars.TNT_CE_ETCD_FALLBACK = value

        t.assert_equals(snapshot.settings().fallback, false, 'значение ' .. value)
    end
end

-- Откат включается привычными написаниями «да», а «не задана» — умолчание.
g.test_fallback_can_be_enabled_explicitly = function()
    local snapshot = load_snapshot()

    for _, value in ipairs({ 'on', 'true', 'YES', '1', 'null' }) do
        vars.TNT_CE_ETCD_FALLBACK = value

        t.assert_equals(snapshot.settings().fallback, true, 'значение ' .. value)
    end
end

--- Отказ `tnt-env` на выключатель, не говорящий ни да, ни нет.
---@param value string Значение выключателя
---@return string
local function unclear_fallback(value)
    return (
        'переменная окружения TNT_CE_ETCD_FALLBACK — логическое значение '
        .. '(true, false, 1, 0, yes, no, on, off), а не «%s»'
    ):format(value)
end

-- Значение, не говорящее ни да, ни нет, — отказ, а не молча включённый
-- откат: опечатка в выключателе иначе обнаружилась бы в день аварии etcd.
g.test_unclear_fallback_value_is_refused = function()
    local snapshot = load_snapshot()

    for _, value in ipairs({ 'да', '', 'выкл' }) do
        vars.TNT_CE_ETCD_FALLBACK = value

        t.assert_error_msg_equals(unclear_fallback(value), snapshot.settings)
    end
end

-- Пустые путь и выключатель в `.env` читаются по-разному — решение
-- владельца (16.09.2026): пустой путь — умолчание, пустой выключатель —
-- отказ. Строка `TNT_CE_ETCD_FALLBACK=` в файле — та самая,
-- что до перехода на `tnt-env` молча включала откат; узел с ней теперь
-- не стартует, и проверка держит это в том виде, в каком строку пишут
-- при развёртывании, — файлом, а не окружением процесса.
g.test_blank_switch_in_env_file_is_refused_while_blank_path_is_default = function()
    local files = { ['.env'] = 'TNT_CE_ETCD_SNAPSHOT=\n' }
    local snapshot = helper.snapshot(vars, files)

    t.assert_equals(snapshot.settings(), { path = 'var/etcd-config-snapshot.yaml', fallback = true })

    files['.env'] = 'TNT_CE_ETCD_SNAPSHOT=\nTNT_CE_ETCD_FALLBACK=\n'

    t.assert_error_msg_equals(unclear_fallback(''), snapshot.settings)
end
