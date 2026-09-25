--- Подключение источника etcd к каркасу расширений.
---
--- Модуль перечисляется в TNT_CE_EXTENSIONS; каркас требует его при
--- инициализации конфигурации, а загрузка регистрирует заявку: открыть
--- узлы config.etcd в схеме и добавить источник.
---
--- Открывается ровно один путь. Узлы iproto.listen и iproto.advertise
--- ради mTLS не открываются нарочно: транспортного SSL в Community
--- Edition нет вовсе, и конфигурацию с ним приняли бы, а работать она
--- не стала бы.

local extras = require('tnt.ce.extras')

extras.register('etcd', {
    relax_prefixes = { 'config.etcd' },

    source = function()
        return require('tnt.ce.etcd.source').new({ name = 'etcd' })
    end,
})

return {
    source = require('tnt.ce.etcd.source'),
    client = require('tnt.ce.etcd.client'),
    snapshot = require('tnt.ce.etcd.snapshot'),
}
