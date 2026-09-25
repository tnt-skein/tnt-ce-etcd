--- Тест подключения источника к каркасу расширений.
---
--- Проверяется заявка: какие пути схемы открываются и создаётся ли источник.
--- Список путей важен сам по себе: лишний открытый узел — например, iproto
--- ради mTLS, которого в Community Edition нет вовсе, — приняли бы молча,
--- а работать он не стал бы.

local t = require('luatest')

local helper = dofile('test/helper.lua')
local testing = helper.testing

local g = t.group('tnt.ce.etcd.registration')

--- Точка подключения пакета.
local ENTRY = helper.ENTRY

g.after_each(function()
    testing.unload_sources(ENTRY)
end)

--- Загружает точку подключения на чистом реестре и возвращает заявки.
---@return table[] entries
local function load_and_capture()
    local extras = require('tnt.ce.extras')
    extras._reset()

    local captured = {}
    local original_register = extras.register

    extras.register = function(name, spec)
        table.insert(captured, { name = name, spec = spec })
        return original_register(name, spec)
    end

    local ok, err = pcall(testing.load_sources, ENTRY, 'tnt.ce.etcd')
    extras.register = original_register
    extras._reset()

    assert(ok, tostring(err))
    return captured
end

-- Расширение регистрируется под именем etcd.
g.test_registers_under_etcd_name = function()
    local entries = load_and_capture()

    t.assert_equals(#entries, 1)
    t.assert_equals(assert(entries[1]).name, 'etcd')
end

-- Открывается ровно один путь схемы и именно config.etcd.
g.test_relaxes_only_config_etcd = function()
    local entries = load_and_capture()

    t.assert_equals(assert(entries[1]).spec.relax_prefixes, { 'config.etcd' })
end

-- Фабрика создаёт источник, отвечающий контракту ядра.
g.test_source_factory_builds_valid_source = function()
    local entries = load_and_capture()

    helper.assert_source_contract(assert(entries[1]).spec.source())
end

-- Пакет отдаёт свои модули наружу: их зовут тесты и диагностика.
g.test_exposes_modules = function()
    local extras = require('tnt.ce.extras')
    extras._reset()

    local exported = testing.load_sources(ENTRY, 'tnt.ce.etcd')
    extras._reset()

    t.assert_type(exported.source, 'table')
    t.assert_type(exported.client, 'table')
    t.assert_type(exported.snapshot, 'table')
end
