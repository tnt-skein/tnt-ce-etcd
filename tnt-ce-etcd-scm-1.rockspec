rockspec_format = '3.0'

package = 'tnt-ce-etcd'
version = 'scm-1'

source = {
    url = 'git+https://github.com/tnt-skein/tnt-ce-etcd.git',
    branch = 'main',
}

description = {
    summary = 'etcd как источник конфигурации кластера для Tarantool Community Edition',
    detailed = [[
        Читает конфигурацию кластера из etcd по HTTP-шлюзу API v3 и отдаёт
        её ядру Tarantool. В Enterprise такой источник встроен, а в Community
        Edition раздел config.etcd отвергается схемой: пакет открывает ровно
        этот раздел через tnt-ce-extras и регистрирует источник. Раздел
        читается по схеме ядра и под её именами — endpoints, prefix,
        username, password, ssl.*, http.request.*, watchers.*, — так что
        конфигурация переезжает на Enterprise как есть.

        При недоступном etcd инстанс поднимается на локальном снимке
        последней успешно прочитанной конфигурации и сообщает, что она
        устарела. Снимок подменяется атомарно, со сбросом на диск
        (tnt-fs): ни обрыв записи, ни отключение питания не оставят
        на его месте обрезанного или пустого файла. Откат выключается
        переменной TNT_CE_ETCD_FALLBACK, путь снимка задаёт
        TNT_CE_ETCD_SNAPSHOT; обе читаются через tnt-env, так что годится
        и строка в .env. И .env, и относительный путь снимка считаются
        от рабочего каталога узла (process.work_dir) — и до box.cfg,
        и после.

        Правку ключа источник замечает опросом и просит ядро перечитать
        конфигурацию, а ядру рассказывает ревизию ключа: так узел отличает
        прочитанную конфигурацию от применённой. Узел, поднятый на снимке,
        тем же опросом замечает вернувшийся etcd и сходит со снимка сам.
        Каждый заход наблюдения идёт в своей области контекста с новым
        request_id.

        Подключается перечислением tnt.ce.etcd в TNT_CE_EXTENSIONS. Только
        для Community Edition. Покрытие строк и убитых мутантов — 100 %.
    ]],
    homepage = 'https://github.com/tnt-skein/tnt-ce-etcd',
    issues_url = 'https://github.com/tnt-skein/tnt-ce-etcd/issues',
    maintainer = 'tnt-skein',
    license = 'MIT',
    labels = { 'tarantool', 'config', 'configuration', 'etcd', 'community-edition' },
}

dependencies = {
    'lua >= 5.1',
    -- Бросок без места: отказ источника читает оператор в выводе старта
    -- узла, и приписка строки пакета там только мешает.
    'tnt-must',
    -- Точка входа ядра и реестр расширений: открывает config.etcd в схеме
    -- и регистрирует источник.
    'tnt-ce-extras',
    -- Своя область с новым request_id на каждый заход наблюдения.
    'tnt-context',
    -- Путь снимка и выключатель отката из окружения и .env рабочего каталога.
    'tnt-env',
    -- Опознаватель захода наблюдения.
    'tnt-id',
    -- Журнал, который прячет пароль в адресе: адрес хранилища попадает
    -- в причину отказа etcd.
    'tnt-log',
    -- Вопрос «box уже настроен?» — внешней зависимостью, подменяемой в проверках.
    'tnt-external',
    -- Снимок: атомарная подмена со сбросом на диск, чтение и время правки.
    'tnt-fs',
}

build = {
    type = 'builtin',
    modules = {
        ['tnt.ce.etcd'] = 'tnt/ce/etcd.lua',
        ['tnt.ce.etcd.source'] = 'tnt/ce/etcd/source.lua',
        ['tnt.ce.etcd.watch'] = 'tnt/ce/etcd/watch.lua',
        ['tnt.ce.etcd.client'] = 'tnt/ce/etcd/client.lua',
        ['tnt.ce.etcd.snapshot'] = 'tnt/ce/etcd/snapshot.lua',
    },
}
