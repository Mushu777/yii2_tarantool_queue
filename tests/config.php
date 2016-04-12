<?php

$config = [
    'tarantool' => [
        'host'     => 'localhost',
        'port'     => '3301',
        'username' => 'test',
        'password' => 'test',
        'options'  => [],
    ]
];

if (is_file(__DIR__ . '/config.local.php')) {
    include(__DIR__ . '/config.local.php');
}

return $config;